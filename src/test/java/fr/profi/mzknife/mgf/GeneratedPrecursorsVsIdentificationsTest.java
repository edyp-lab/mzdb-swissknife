package fr.profi.mzknife.mgf;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.reader.iterator.SpectrumIterator;
import fr.profi.mzdb.io.writer.mgf.IsolationWindowPrecursorExtractor_v3_6;
import fr.profi.mzdb.io.writer.mgf.MgfPrecursor;
import fr.profi.mzdb.model.IonMobilityMode;
import fr.profi.mzdb.model.IonMobilityType;
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
public class GeneratedPrecursorsVsIdentificationsTest {

  final static Logger logger = LoggerFactory.getLogger(GeneratedPrecursorsVsIdentificationsTest.class);
  private final String CONCORDANT = "concordant";
  private final String UP_QX = "up QX";
  private final String UP_MZDB = "up mzdb";

  private Metric metric = new Metric("GeneratedPrecursorsVsIdentificationsTest");

  @Test
  public void testMGFGeneration() {

    try {

      float mzTol = 10.0f;
      final List<Identification> idents = Identification.fromFile(SearchIdentifiedPrecursorsTest.class.getResource("/run_2790.csv").getFile());
      logger.info("nb identifications = {}", idents.size());
      Map<Integer, List<Identification>> identsByScan = idents.stream().collect(Collectors.groupingBy(i -> i.scan));

      HashSet<Integer> matches = new HashSet<>();
      String mzdbFilePath = "C:/Local/bruley/Data/Proline/Data/mzdb/Exploris/Xpl1_002790.mzDB";

      MzDbReader mzDbReader = new MzDbReader(mzdbFilePath, true);
      mzDbReader.enablePrecursorListLoading();
      mzDbReader.enableScanListLoading();

      final IonMobilityMode ionMobilityMode = mzDbReader.getIonMobilityMode();
      IsolationWindowPrecursorExtractor_v3_6 precComputer = new IsolationWindowPrecursorExtractor_v3_6(mzTol, ionMobilityMode.getIonMobilityMode() == IonMobilityType.FAIMS);

      logger.info("nb identifications = {}", idents.size());
      logger.info("nb MS2 scans = {}", mzDbReader.getSpectraCount(2));

      // Iterate MSn spectra
      SpectrumIterator spectrumIterator = new SpectrumIterator(mzDbReader, mzDbReader.getConnection(), 2);
      while (spectrumIterator.hasNext()) {

        SpectrumHeader spectrumHeader = spectrumIterator.next().getHeader();
        MgfPrecursor[] mgfPrecursors = precComputer.getMgfPrecursors(mzDbReader, spectrumHeader);

        int  rank = 0;

        for(MgfPrecursor mgfPrecursor : mgfPrecursors) {

          metric.incr("mgf.entry");

          double mgfMz = mgfPrecursor.getPrecMz(); //precComputer.getPrecursorMz(mzDbReader, spectrumHeader)
          int mgfZ = mgfPrecursor.getCharge(); //precComputer.getPrecursorCharge(mzDbReader, spectrumHeader)

          if (identsByScan.containsKey(spectrumHeader.getInitialId())) {
            Identification ident = identsByScan.get(spectrumHeader.getInitialId()).get(0);

            if ((ident.charge == mgfZ) && (Math.abs(1e6 * (ident.moz - mgfMz) / mgfMz) < mzTol)) {

              if (matches.contains(spectrumHeader.getInitialId())) {
                metric.incr("match.duplicated");
              } else {
                matches.add(spectrumHeader.getInitialId());
                metric.incr("match");
                switch(ident.status) {
                  case CONCORDANT:
                    metric.incr("match.was_concordant");
                    break;
                  case UP_QX:
                    metric.incr("match.was_up_QX");
                    break;
                  case UP_MZDB:
                    metric.incr("match.was_up_mzdb");
                    break;
                }

                if (ident.status.equals(UP_QX) && rank >= 1) {
                  metric.incr("match.from_rank_n");
                }
              }
            }
          }
          rank = rank + 1;
        }

        // count number of precursors per spectrum

        metric.incr("nb_precursors_generated." + mgfPrecursors.length);

        // test precursor list generated from the current spectrumId

        if (identsByScan.containsKey(spectrumHeader.getInitialId())) {
          final Identification ident = identsByScan.get(spectrumHeader.getInitialId()).get(0);
          final Optional<MgfPrecursor> matching = Arrays.stream(mgfPrecursors).filter(prec -> (ident.charge == prec.getCharge()) && (Math.abs(1e6 * (ident.moz - prec.getPrecMz()) / prec.getPrecMz()) < mzTol)).findFirst();
          if (!matching.isPresent()) {
            metric.incr("lost_match");

            if (ident.status.equals(CONCORDANT) || ident.status.equals(UP_MZDB)) {
              metric.incr("lost_match.from_legacy_mgf_generation");
              if (Math.abs(1e6 * (ident.moz - spectrumHeader.getPrecursorMz()) / spectrumHeader.getPrecursorMz()) < mzTol) {
                metric.incr("lost_match.from_legacy_mgf_generation.header_prec_mz_was_correct");
                //                if (ident.charge == spectrumHeader.getPrecursorCharge) {
                //                  logger.info("exemple of lost match: {}, {}, {}", ident.scan, ident.moz, ident.charge)
                //                }
              }
            }

            if (ident.status.equals(CONCORDANT)) {
              metric.incr("lost_match.was_concordant");
            } else if (ident.status.equals(UP_MZDB)) {
              metric.incr("lost_match.was_up_mzdb");
            } else if (ident.status.equals(UP_QX)) {
              metric.incr("lost_match.was_up_qx");
            }
          } else {
            // for matched identifications, count how many precursors are generated
            metric.incr("matched.nb_precursors_generated." + mgfPrecursors.length);
          }
        }

      }

      logger.info(metric.toString());
      precComputer.dumpMetrics();

    }  catch (IOException e) {
      throw new RuntimeException(e);
    } catch (SQLiteException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }


}
