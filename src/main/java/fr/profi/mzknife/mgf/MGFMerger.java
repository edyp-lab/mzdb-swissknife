package fr.profi.mzknife.mgf;

import fr.profi.mzknife.util.MGFUtils;
import fr.profi.mzscope.InvalidMGFFormatException;
import fr.profi.mzscope.MGFReader;
import fr.profi.mzscope.MSMSSpectrum;
import fr.profi.util.metrics.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MGFMerger extends MGFRewriter {

  private final static Logger LOG = LoggerFactory.getLogger(MGFMerger.class);

  private boolean m_filterSpectrum = true;
  private boolean m_replaceFragments = true;

  private final Metric metric = new Metric(MGFMerger.class.getName());
  protected Map<String, List<MSMSSpectrum>> m_indexedFragmentSpectra = new HashMap<>();


  public MGFMerger(File srcPrecursorFile, File srcFragmentsFile, File m_dstFile) throws InvalidMGFFormatException, IOException {
    super(srcPrecursorFile, m_dstFile);

    // index the entries of the second mgf by scan (scan numbers are extracted from the title)

    MGFReader reader = new MGFReader(srcFragmentsFile);
    List<MSMSSpectrum> fragmentSpectra = reader.readAllSpectrum();
    for (MSMSSpectrum spectrum : fragmentSpectra) {
        metric.incr("fragments.file.scan_count");
        String scan = MGFUtils.getScanAsString(spectrum);
        if ((scan != null) && !scan.trim().isEmpty()) {
          List<MSMSSpectrum> list = m_indexedFragmentSpectra.getOrDefault(scan, new ArrayList<>(2));
          list.add(spectrum);
          m_indexedFragmentSpectra.put(scan, list);
        } else {
          LOG.warn("No index or scan found in title : "+spectrum.getAnnotation("TITLE"));
          metric.incr("fragments.file.scan_not_found");
        }
    }

    metric.setCounter("fragments.file.indexed_scans", m_indexedFragmentSpectra.size());
    long multipleMatchCount = m_indexedFragmentSpectra.entrySet().stream().filter(e -> e.getValue().size() > 1).count();
    metric.setCounter("fragments.file.multiple_match_count", (int)multipleMatchCount);
    LOG.info("index created : {} entries", m_indexedFragmentSpectra.size());
  }

  public void setFilterSpectrum(boolean filter) {
    this.m_filterSpectrum = filter;
  }

  public void setReplaceFragments(boolean replaceFragments) {
    this.m_replaceFragments = replaceFragments;
  }

  public void dumpMetric() {
    LOG.info(metric.toString());
  }

  protected MSMSSpectrum getSpectrum2Export(MSMSSpectrum inSpectrum){

    metric.incr("precursors.file.scan_count");
    String scan = MGFUtils.getScanAsString(inSpectrum);

    if ((scan != null) && !scan.trim().isEmpty()) {
      metric.incr("precursors.file.scan_not_found");
    }

    if (m_indexedFragmentSpectra.containsKey(scan)) {

      List<MSMSSpectrum> spectrumList = m_indexedFragmentSpectra.get(scan);
      MSMSSpectrum fragmentSpectrum = spectrumList.get(0);

      if (m_replaceFragments) {
        double[] masses = fragmentSpectrum.getMassValues();
        double[] intensities = fragmentSpectrum.getIntensityValues();

        MSMSSpectrum outSpectrum = new MSMSSpectrum(
                inSpectrum.getPrecursorMz(),
                inSpectrum.getPrecursorIntensity(),
                inSpectrum.getPrecursorCharge(),
                inSpectrum.getRetentionTime());

        inSpectrum.getAnnotations().forEachRemaining(a -> outSpectrum.setAnnotation(a, inSpectrum.getAnnotation(a)));

        for (int i = 0; i < masses.length; i++) {
          outSpectrum.addPeak(masses[i], intensities[i]);
        }

        return outSpectrum;
      } else {
        return inSpectrum;
      }

    } else {
      LOG.info("no corresponding scan in fragmentsMGF");
      metric.incr("precursors.file.no_matching_scan_in_fragments_file");
      return m_filterSpectrum ? null : inSpectrum;
    }

  }

}
