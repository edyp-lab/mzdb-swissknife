package fr.profi.mzknife.mgf;

import fr.profi.mzscope.InvalidMGFFormatException;
import fr.profi.mzscope.MGFConstants;
import fr.profi.mzscope.MGFReader;
import fr.profi.mzscope.MSMSSpectrum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MGFMetrics {

  private static final CharSequence DELIMITER = "\t";

  protected final List<MSMSSpectrum> m_msmsSpectra;
  protected final File m_dstFile;

  private final static Logger LOG = LoggerFactory.getLogger(MGFMetrics.class);

  protected MGFMetrics() {
    this.m_msmsSpectra = Collections.emptyList();
    this.m_dstFile = null;
  }

  public MGFMetrics(File srcFile, File m_dstFile) throws InvalidMGFFormatException {
    MGFReader reader = new MGFReader();
    this.m_msmsSpectra = reader.read(srcFile);
    this.m_dstFile = m_dstFile;
  }

  public void dumpMGFMetrics() throws IOException {

    BufferedWriter writer = new BufferedWriter(new FileWriter(m_dstFile));
    String[] columns = { "scan", "precursors_count", "fragment_peaks_count", "charge"};

    writer.write(Arrays.stream(columns).collect(Collectors.joining(DELIMITER)));
    writer.newLine();

    final Map<String, List<MSMSSpectrum>> mapToScan = m_msmsSpectra.stream().collect(Collectors.groupingBy(spectrum -> (String)spectrum.getAnnotation(MGFConstants.SCANS)));

    for (Map.Entry<String, List<MSMSSpectrum>> entry : mapToScan.entrySet()) {

      StringBuilder strBuilder = new StringBuilder();
      strBuilder.append(entry.getKey()).append(DELIMITER);
      strBuilder.append(entry.getValue().size()).append(DELIMITER);
      final MSMSSpectrum spectrum = entry.getValue().get(0);
      strBuilder.append(spectrum.peaksCount()).append(DELIMITER);
      strBuilder.append(spectrum.getPrecursorCharge());

      writer.write(strBuilder.toString());
      writer.newLine();
      writer.flush();

    }

    writer.flush();
    writer.close();
  }


}
