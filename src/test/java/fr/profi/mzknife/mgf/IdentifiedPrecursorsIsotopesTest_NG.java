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
import java.util.*;
import java.util.stream.Collectors;

@Ignore
public class IdentifiedPrecursorsIsotopesTest_NG {

  final static Logger logger = LoggerFactory.getLogger(IdentifiedPrecursorsIsotopesTest_NG.class);

  private Metric metric = new Metric("SearchIdentifiedPrecursorsTest_NG");
  private String[] annotations = {"scan.number", "scan.rt", "ident.status", "ident.duplicated.status", "ident.moz", "ident.charge", "ident.score_max", "found",
    "ident.initial.rank", "ident.initial.moz" , "ident.initial.intensity", "cause", "ident.isotope.shift.1", "ident.isotope.shift.2", "ident.isotope.shift.3", "ident.isotope.shift.4",
    "ident.isotope.shift.ppm.1", "ident.isotope.shift.ppm.2", "ident.isotope.shift.ppm.3", "ident.isotope.shift.ppm.4",
    "ident.isotope.shift.abs.1", "ident.isotope.shift.abs.2", "ident.isotope.shift.abs.3", "ident.isotope.shift.abs.4"};


  /**
   * Extract isotopic peaks distance statistics for each identified precursor found in the run_2790_v3.6_NG.csv resource file.
   */
  @Test
  public void testMGFGeneration() {

    float mzTol = 10.0f;

    try {

      List<Identification_NG> idents = Identification_NG.fromFile(IdentifiedPrecursorsIsotopesTest_NG.class.getResource("/run_2790_v3.6_NG.csv").getFile());

      logger.info("nb identifications = {}", idents.size());
      Map<Integer, List<Identification_NG>> identsByScan = idents.stream().collect(Collectors.groupingBy(i -> i.scan));

      String mzdbFilePath = "C:/Local/bruley/Data/Proline/Data/mzdb/Exploris/Xpl1_002790.mzDB";

      BufferedWriter fw = new BufferedWriter(new FileWriter(new File((new File(mzdbFilePath)).getParentFile(), "precursors_isotopes_stats.txt" )));
      fw.write(Arrays.stream(annotations).collect(Collectors.joining("\t")));
      fw.newLine();

      MzDbReader mzDbReader = new MzDbReader(mzdbFilePath, true);
      mzDbReader.enablePrecursorListLoading();
      mzDbReader.enableScanListLoading();

      final IonMobilityMode ionMobilityMode = mzDbReader.getIonMobilityMode();
      IsolationWindowPrecursorExtractor_v3_6 precComputer = new IsolationWindowPrecursorExtractor_v3_6(mzTol,(ionMobilityMode != null && ionMobilityMode.getIonMobilityMode() == IonMobilityType.FAIMS));

      logger.info("nb identifications = {}", idents.size());
      logger.info("nb MS2 scans = {}", mzDbReader.getSpectraCount(2));

      final List<Map.Entry<Integer, List<Identification_NG>>> sortedEntries = identsByScan.entrySet().stream().sorted(Comparator.comparingInt(e -> e.getKey())).collect(Collectors.toList());

      for( Map.Entry<Integer, List<Identification_NG>> entry : sortedEntries) {
        Integer scan = entry.getKey();
        Identification_NG identification = entry.getValue().get(0);
        
        SpectrumHeader spectrumHeader = mzDbReader.getSpectrumHeader(scan);
        Map<String, Object> map = scala.collection.JavaConverters.mapAsJavaMapConverter(precComputer.extractPrecursorIsotopesStats(mzDbReader, spectrumHeader, identification.bestMoz, identification.bestCharge, mzTol)).asJava();
        map = new HashMap<>(map);

        map.put("scan.number", scan);
        map.put("scan.rt", spectrumHeader.getElutionTime() / 60.0);
        map.put("ident.status", identification.status());
        map.put("ident.duplicated.status", identification.duplicatedStatus());
        map.put("ident.moz", identification.bestMoz);
        map.put("ident.charge", identification.bestCharge);
        map.put("ident.score_max", identification.bestScore);

        Utils.dumpStats(map, fw, annotations);

        if (identification.worstQuery >= 0) {

          map  = scala.collection.JavaConverters.mapAsJavaMapConverter(precComputer.extractPrecursorIsotopesStats(mzDbReader, spectrumHeader, identification.worstMoz, identification.worstCharge, mzTol)).asJava();
          map = new HashMap<>(map);
          map.put("scan.number", scan);
          map.put("scan.rt", spectrumHeader.getElutionTime() / 60.0);
          map.put("ident.status", identification.status());
          map.put("ident.duplicated.status", identification.duplicatedStatus());
          map.put("ident.moz", identification.worstMoz);
          map.put("ident.charge", identification.worstCharge);
          map.put("ident.score_max", identification.worstScore);

          Utils.dumpStats(map, fw, annotations);
        }
      }

      logger.info(metric.toString());
      precComputer.dumpMetrics();

      fw.flush();
      fw.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (SQLiteException e) {
      throw new RuntimeException(e);
    }
  }


}
