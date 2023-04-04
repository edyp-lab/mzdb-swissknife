package fr.profi.mzknife.mgf;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.writer.mgf.IsolationWindowPrecursorExtractor_v3_6;
import fr.profi.mzdb.model.IonMobilityMode;
import fr.profi.mzdb.model.IonMobilityType;
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
 *   Search the moz of the identified precursors in the mzdb file by targeting the scan number and extract some metrics about
 *   signal found in the raw data. The extracted stats are dumped into a txt file.
 */
public class SearchIdentifiedPrecursorsTest {

  final static Logger logger = LoggerFactory.getLogger(SearchIdentifiedPrecursorsTest.class);

  private Metric metric = new Metric("SearchIdentifiedPrecursorsTest");
  private String[] annotations = {"scan.number", "scan.rt", "ident.status", "ident.moz", "ident.charge", "ident.score_max", "ident.score_from", "found",
    "ident.initial.rank", "ident.initial.moz" , "ident.initial.intensity", "ident.prediction.moz", "ident.prediction.charge", "ident.prediction.note", "ident.prediction.intensity", "ident.prediction.rank",
    "rank0.initial.intensity", "rank0.initial.moz", "rank0.prediction.moz", "rank0.prediction.charge", "rank0.prediction.note", "rank0.prediction.rank",
    "swcenter.initial.rank", "swcenter.initial.moz", "swcenter.initial.intensity", "swcenter.prediction.moz", "swcenter.prediction.charge", "swcenter.prediction.note", "swcenter.prediction.rank",
    "cause", "header.moz", "header.charge", "header.found", "sw_center.moz"};


  @Test
  public void testMGFGeneration() {

    try {

    float mzTol = 10.0f;

      final List<Identification> idents = Identification.fromFile(SearchIdentifiedPrecursorsTest.class.getResource("/run_2790.csv").getFile());
      logger.info("nb identifications = {}", idents.size());
      Map<Integer, List<Identification>> identsByScan = idents.stream().collect(Collectors.groupingBy(i -> i.scan));

      String mzdbFilePath = "C:/Local/bruley/Data/Proline/Data/mzdb/Exploris/Xpl1_002790.mzDB";

      BufferedWriter fw = new BufferedWriter(new FileWriter(new File((new File(mzdbFilePath)).getParentFile(), "precursors_stats_full_v3_6.txt" )));
      fw.write(Arrays.stream(annotations).collect(Collectors.joining("\t")));
      fw.newLine();

      MzDbReader mzDbReader = new MzDbReader(mzdbFilePath, true);
      mzDbReader.enablePrecursorListLoading();
      mzDbReader.enableScanListLoading();

      logger.info("nb identifications = {}", idents.size());
      logger.info("nb MS2 scans = {}", mzDbReader.getSpectraCount(2));

      final IonMobilityMode ionMobilityMode = mzDbReader.getIonMobilityMode();
      IsolationWindowPrecursorExtractor_v3_6 precComputer = new IsolationWindowPrecursorExtractor_v3_6(mzTol, (ionMobilityMode != null && ionMobilityMode.getIonMobilityMode() == IonMobilityType.FAIMS));
      
      for( Map.Entry<Integer, List<Identification>> entry : identsByScan.entrySet()) {
        Integer scan = entry.getKey();
        Identification identification = entry.getValue().get(0);
        
        SpectrumHeader spectrumHeader = mzDbReader.getSpectrumHeader(scan);
        Map<String, Object> map = scala.collection.JavaConverters.mapAsJavaMapConverter(precComputer.extractPrecursorStats(mzDbReader, spectrumHeader, identification.moz, mzTol)).asJava();
        map = new HashMap<>(map);
        map.put("scan.number",  scan);
        map.put("scan.rt",  spectrumHeader.getElutionTime()/60.0);
        map.put("ident.status",  identification.status);
        map.put("ident.moz",  identification.moz);
        map.put("ident.charge",  identification.charge);
        map.put("ident.score_max",  identification.scoreMax);
        map.put("ident.score_from",  identification.scoreFrom);
  
        Utils.dumpStats(map, fw, annotations);
      }

  
      logger.info(metric.toString());
      precComputer.dumpMetrics();
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
