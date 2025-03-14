package fr.profi.mzknife.mgf;

import com.almworks.sqlite4java.SQLiteException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.reader.iterator.SpectrumIterator;
import fr.profi.mzdb.io.writer.mgf.MgfBoostPrecursorExtractor;
import fr.profi.mzdb.io.writer.mgf.MgfPrecursor;
import fr.profi.mzdb.io.writer.mgf.ScanSelectorModes;
import fr.profi.mzdb.model.IonMobilityMode;
import fr.profi.mzdb.model.SpectrumHeader;
import fr.profi.util.metrics.Metric;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Ignore
/**
 *   Compare the Precursor's list generated from the mzdb file with the annotated identified scans
 */
public class GeneratedPrecursorsVsIdentificationsTest_NG {

  final static Logger logger = LoggerFactory.getLogger(GeneratedPrecursorsVsIdentificationsTest_NG.class);

  private Metric metric = new Metric("GeneratedPrecursorsVsIdentificationsTest");

  @Test
  public void testMGFGeneration() {

    try {
      float mzTol = 10.0f;

      final Config config = ConfigFactory.load();
      logger.info("config max charge =  " + config.getInt("maxIsotopicChargeState"));


      List<Identification_NG> idents = Identification_NG.fromFile(IdentifiedPrecursorsIsotopesTest_NG.class.getResource("/run_2790_v3.6_NG.csv").getFile());
      Map<Integer, List<Identification_NG>> identsByScan = idents.stream().collect(Collectors.groupingBy(i -> i.scan));

      HashSet<Integer> matches = new HashSet<>();
      String mzdbFilePath = "C:/Local/bruley/Data/Proline/Data/mzdb/Exploris/Xpl1_002790.mzDB";

      MzDbReader mzDbReader = new MzDbReader(mzdbFilePath, true);
      mzDbReader.enablePrecursorListLoading();
      mzDbReader.enableScanListLoading();

      final IonMobilityMode ionMobilityMode = mzDbReader.getIonMobilityMode();
      MgfBoostPrecursorExtractor precComputer = new MgfBoostPrecursorExtractor(mzTol,true, true, 1, 0.2f, ScanSelectorModes.SAME_CYCLE(), 0.0, 100);

      logger.info("nb identifications = {}", idents.size());
      logger.info("nb MS2 scans = {}", mzDbReader.getSpectraCount(2));

      // Iterate MSn spectra
      SpectrumIterator spectrumIterator = new SpectrumIterator(mzDbReader, mzDbReader.getConnection(), 2);
      while (spectrumIterator.hasNext()) {

        SpectrumHeader spectrumHeader = spectrumIterator.next().getHeader();
        final MgfPrecursor[] mgfPrecursors = precComputer.getMgfPrecursors(mzDbReader, spectrumHeader);
        int rank = 0;

        for(MgfPrecursor mgfPrecursor : mgfPrecursors) {

          metric.incr("mgf.entry");

          double mgfMz = mgfPrecursor.getPrecMz(); //precComputer.getPrecursorMz(mzDbReader, spectrumHeader)
          int mgfZ = mgfPrecursor.getCharge(); //precComputer.getPrecursorCharge(mzDbReader, spectrumHeader)

          if (identsByScan.containsKey(spectrumHeader.getInitialId())) {
            final Identification_NG ident = identsByScan.get(spectrumHeader.getInitialId()).get(0);
            if ((ident.bestCharge == mgfZ) && (Math.abs(1e6 * (ident.bestMoz - mgfMz) / mgfMz) < mzTol)) {

              if (matches.contains(spectrumHeader.getInitialId())) {
                metric.incr("match.duplicated");
              } else {
                matches.add(spectrumHeader.getInitialId());
                metric.incr("match");

              }
            }
          }
          rank += 1;
        }

        // count number of precursors per spectrum

        metric.incr("nb_precursors_generated." + mgfPrecursors.length);

        // test precursor list generated from the current spectrumId

        if (identsByScan.containsKey(spectrumHeader.getInitialId())) {
          Identification_NG ident = identsByScan.get(spectrumHeader.getInitialId()).get(0);
          final Optional<MgfPrecursor> matching = Arrays.stream(mgfPrecursors).filter(prec -> (ident.bestCharge == prec.getCharge()) && (Math.abs(1e6 * (ident.bestMoz - prec.getPrecMz()) / prec.getPrecMz()) < mzTol)).findFirst();
          if (!matching.isPresent()) {
            metric.incr("lost_match");
            if (ident.bestTargetCount > 0) {
              metric.incr("lost_match.was_target");
            }

          } else {
            // for matched identifications, count how many precursors are generated
            metric.incr("matched.nb_precursors_generated." + mgfPrecursors.length);
          }
        }

      }

      logger.info(metric.toString());
      precComputer.dumpMetrics();

    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (SQLiteException e) {
      throw new RuntimeException(e);
    }
  }


}
