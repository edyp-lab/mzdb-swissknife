package fr.profi.mzknife.mgf;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.writer.mgf.IsolationWindowPrecursorExtractor_v3_6;
import fr.profi.mzdb.io.writer.mgf.IsolationWindowPrecursorExtractor_v3_7;
import fr.profi.mzdb.io.writer.mgf.MgfPrecursor;
import fr.profi.mzdb.model.SpectrumHeader;
import fr.profi.util.metrics.Metric;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Ignore
/**
 *   Extract data from the selection widow content
 *   The extracted stats are dumped into a txt file.
 */
public class SelectionWindowContentTest {

  final static Logger logger = LoggerFactory.getLogger(SelectionWindowContentTest.class);

  private Metric metric = new Metric("SelectionWindowContentTest");
  private String[] annotations = {"scan.number", "scan.rt", "cause", "header.moz", "header.charge", "sw_center.moz", "sw_center.lower_offset",
          "sw_center.upper_offset", "sw_content.max_intensity", "sw_center.peak.moz", "sw_center.peak.intensity", "sw_center.peak.rank", "sw_content.precursors.count",
          "sw_content_0.count", "sw_content_1.count", "sw_content_2.count", "sw_content_3.count", "sw_content_4.count", "sw_content_5.count",
          "sw_content_6.count", "sw_content_7.count", "sw_content_8.count", "sw_content_9.count", "sw_content_10.count"};


  @Test
  public void testMGFGeneration() {

    try {

    float mzTol = 10.0f;

      //String mzdbFilePath = "C:/Local/bruley/Data/Proline/Data/mzdb/Exploris/Xpl1_002790.mzDB";
//      String mzdbFilePath = "C:/Local/bruley/Tests/MGF/SW/Xpl1_005145_selec0_8.mzdb";
//      String mzdbFilePath = "C:/Local/bruley/Tests/MGF/SW/Xpl1_005146_selec1_4.mzdb";
//      String mzdbFilePath = "C:/Local/bruley/Tests/MGF/SW/Xpl1_005147_selec2.mzdb";
      String mzdbFilePath = "C:/Local/bruley/Tests/MGF/SW/Xpl1_005148_selec2_6.mzdb";

      String inputFileName = new File(mzdbFilePath).getName();
      inputFileName = inputFileName.substring(0, inputFileName.lastIndexOf("."));
      BufferedWriter fw = new BufferedWriter(new FileWriter(new File((new File(mzdbFilePath)).getParentFile(), "sw_stats_"+ inputFileName+".txt")));

      fw.write(Arrays.stream(annotations).collect(Collectors.joining("\t")));
      fw.newLine();

      IsolationWindowPrecursorExtractor_v3_6 precursorExtractor_v3_6 = new IsolationWindowPrecursorExtractor_v3_6(mzTol);
      IsolationWindowPrecursorExtractor_v3_7 precursorExtractor_v3_7 = new IsolationWindowPrecursorExtractor_v3_7(mzTol);

      MzDbReader mzDbReader = new MzDbReader(mzdbFilePath, true);
      mzDbReader.enablePrecursorListLoading();
      mzDbReader.enableScanListLoading();
      logger.info("nb MS2 scans = {}", mzDbReader.getSpectraCount(2));

      for (SpectrumHeader sh : mzDbReader.getMs2SpectrumHeaders()) {

        Integer scan = sh.getInitialId();

        SpectrumHeader spectrumHeader = mzDbReader.getSpectrumHeader(scan);
        Map<String, Object> map = scala.collection.JavaConverters.mapAsJavaMapConverter(precursorExtractor_v3_6.extractSWStats(mzDbReader, spectrumHeader, mzTol)).asJava();
        map = new HashMap<>(map);
        map.put("scan.number",  scan);
        map.put("scan.rt",  spectrumHeader.getElutionTime()/60.0);

        MgfPrecursor[] mgfPrecursors = precursorExtractor_v3_7.getPossibleMgfPrecursorsFromSW(mzDbReader, spectrumHeader);
        map.put("sw_content.precursors.count",mgfPrecursors.length);

        Utils.dumpStats(map, fw, annotations);
      }

  
      logger.info(metric.toString());
      precursorExtractor_v3_6.dumpMetrics();
      fw.flush();
      fw.close();
  
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (SQLiteException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }


}
