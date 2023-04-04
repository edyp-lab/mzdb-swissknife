package fr.profi.mzknife.mgf;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.reader.iterator.SpectrumIterator;
import fr.profi.mzdb.io.writer.mgf.AnnotatedMgfPrecursor;
import fr.profi.mzdb.io.writer.mgf.IsolationWindowPrecursorExtractor_v3_7;
import fr.profi.mzdb.io.writer.mgf.MgfPrecursor;
import fr.profi.mzdb.model.SpectrumHeader;
import fr.profi.util.metrics.Metric;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Ignore
/**
 *   Search the moz of the identified precursors in the mzdb file by targeting the scan number and extract some metrics about
 *   signal found in the raw data. The extracted stats are dumped into a txt file.
 */
public class PrecursorGenerationTest  {

  final static Logger logger = LoggerFactory.getLogger(PrecursorGenerationTest.class);

  private Metric metric = new Metric("PrecursorGenerationTest");

  @Test
  public void testMGFGeneration() {

    try {
      float mzTol = 10.0f;
      String mzdbFilePath = "C:/Local/bruley/Data/Proline/Data/mzdb/Exploris/Xpl1_002790.mzDB";
      IsolationWindowPrecursorExtractor_v3_7 precComputer = new IsolationWindowPrecursorExtractor_v3_7(mzTol);

      MzDbReader mzDbReader = new MzDbReader(mzdbFilePath, true);
      mzDbReader.enablePrecursorListLoading();
      mzDbReader.enableScanListLoading();

      logger.info("nb MS2 scans = {}", mzDbReader.getSpectraCount(2));

      // Iterate MSn spectra
      SpectrumIterator spectrumIterator = new SpectrumIterator(mzDbReader, mzDbReader.getConnection(), 2);

      while (spectrumIterator.hasNext()) {

        SpectrumHeader spectrumHeader = spectrumIterator.next().getHeader();
        MgfPrecursor[] mgfPrecursors = precComputer.getPossibleMgfPrecursorsFromSW(mzDbReader, spectrumHeader);

        metric.incr("nb_precursors_generated." + mgfPrecursors.length);
        int rank = 0;

         for (MgfPrecursor mgfPrecursor : mgfPrecursors) {

          metric.incr("mgf.entry");
          if (rank == 0) {
            metric.incr("first.was_rank." + ((AnnotatedMgfPrecursor)mgfPrecursor).getAnnotation("rank"));
          }
          rank = rank + 1;
        }

      }

      logger.info(metric.toString());

    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (SQLiteException e) {
      throw new RuntimeException(e);
    }
  }


}
