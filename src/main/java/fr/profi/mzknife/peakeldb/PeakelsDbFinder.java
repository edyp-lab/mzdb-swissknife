package fr.profi.mzknife.peakeldb;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.github.davidmoten.rtree.Entries;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.featuredb.FeatureDbReader;
import fr.profi.mzdb.model.Feature;
import fr.profi.mzdb.model.Peakel;
import fr.profi.mzdb.model.PutativeFeature;
import fr.profi.mzdb.peakeldb.PeakelDbHelper$;
import fr.profi.mzdb.peakeldb.io.PeakelDbReader;
import fr.profi.util.metrics.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Option$;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

import static scala.collection.JavaConversions.asJavaIterable;

public class PeakelsDbFinder {

  private final static Logger LOG = LoggerFactory.getLogger(PeakelsDbFinder.class);
  private static final CharSequence DELIMITER = ";";
  private static final DecimalFormat DF = new DecimalFormat("#");
  private static final int ISOTOPE_PATTERN_HALF_MZ_WINDOW = 5;

  private final File outputFile;
  private final File peakelDbFile;
  private SQLiteConnection connection;


  public PeakelsDbFinder(File peakelDbFile, File outputFile) {
    try {

      this.outputFile = outputFile;
      this.peakelDbFile = peakelDbFile;
      connection = new SQLiteConnection(peakelDbFile);
      connection.openReadonly();

    } catch (SQLiteException e) {
      throw new RuntimeException(e);
    }
  }

  public void matchIdentifiedPsms(List<PutativeFeature> putativeFts, MzDbReader reader, Float mzTolPPM) throws Exception {


    Metric metrics = new Metric("PeakelsDbFinder");

    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
    String[] columns = {"id", "mz", "charge", "rt", "intensity", "peakel_elution_time", "peakel_id", "peakel_mz"};

    writer.write(Arrays.stream(columns).collect(Collectors.joining(DELIMITER)));
    writer.newLine();

    LOG.info("Indexing {} identified PSMs by Cycle", putativeFts.size());
    Map<Integer, List<PutativeFeature>> psmsByCycle = new HashMap<>();

//    List<PutativeFeature>[] psmsByCycle = new List[reader.getCyclesCount()+1];

    for (PutativeFeature putativeFt : putativeFts) {
      int cycle = reader.getSpectrumHeaderForTime(putativeFt.elutionTime(), 2).getCycle();
      psmsByCycle.computeIfAbsent(cycle, k -> new ArrayList<>()).add(putativeFt);

//      if (psmsByCycle[cycle] == null) {
//        psmsByCycle[cycle] = new ArrayList<>();
//      }
//      psmsByCycle[cycle].add(putativeFt);
    }

    LOG.info("Searching for {} identified PSMs", putativeFts.size());
    long start = System.currentTimeMillis();
    Iterator<Peakel> peakelsIt = asJavaIterable(PeakelDbReader.loadAllPeakels(connection, 800000)).iterator();
    int peakelsCount = 0;
    while (peakelsIt.hasNext()) {
      Peakel peakel = peakelsIt.next();
      peakelsCount++;
      double mzTolDa = peakel.getMz() * mzTolPPM / 1e6;
      final int firstCycle = reader.getSpectrumHeaderForTime(peakel.getFirstElutionTime(), 1).getCycle();
      final int lastCycle = reader.getSpectrumHeaderForTime(peakel.getLastElutionTime(), 1).getCycle();
      int currentCycle = firstCycle;
      while (currentCycle <= lastCycle) {
        final List<PutativeFeature> featureList = psmsByCycle.get(currentCycle); //psmsByCycle[currentCycle];
        if (featureList != null) {
          final List<PutativeFeature> matchingFeatures = featureList.stream().filter(f -> Math.abs(f.getMz() - peakel.getMz()) <= mzTolDa).collect(Collectors.toUnmodifiableList());

          for (PutativeFeature f : matchingFeatures) {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append(f.id()).append(DELIMITER);
            strBuilder.append(f.getMz()).append(DELIMITER);
            strBuilder.append(f.charge()).append(DELIMITER);
            strBuilder.append(f.elutionTime()).append(DELIMITER);
            strBuilder.append(peakel.getApexIntensity()).append(DELIMITER);
            strBuilder.append(peakel.getElutionTime()).append(DELIMITER);
            strBuilder.append(peakel.id()).append(DELIMITER);
            strBuilder.append(peakel.getMz());

            writer.write(strBuilder.toString());
            writer.newLine();
            writer.flush();
            metrics.incr("matching_peakels_found");
          }
        }
        currentCycle++;
      }
      if ((peakelsCount % 50000) == 0) LOG.info("scanned peakels : {}", peakelsCount);
    }
    LOG.info("Total Duration : {} ms for {} peakels ", (System.currentTimeMillis() - start), peakelsCount);
    LOG.info("Metrics : {}", metrics.toString());
  }

  public void matchPutativeFeatures(List<PutativeFeature> putativeFts, File featureDbFile, Float mzTolPPM) throws IOException {

    //
    // Build assigned oeakels list
    //
    scala.collection.immutable.Set<Object> assignedPeakelIds = null;
    try {
      if (featureDbFile != null) {
        SQLiteConnection ft_connection = new SQLiteConnection(featureDbFile);
        ft_connection.openReadonly();
        assignedPeakelIds = FeatureDbReader.loadAssignedPeakelIds(ft_connection);
        ft_connection.dispose();
      }

    } catch (SQLiteException e) {
      LOG.error("Error while reading featureDb file " + featureDbFile.getAbsolutePath(), e);
    }

    //
    // Index peakels into RTree
    //
    LOG.info("start reading peakels ...");

    Iterator<Peakel> peakelsIt = asJavaIterable(PeakelDbReader.loadAllPeakels(connection, 800000)).iterator();
    List<Entry<Integer, Point>> entries = new ArrayList<Entry<Integer, Point>>();

    int peakelsCount = 0;
    RTree<Integer, Point> rTree = RTree.star().maxChildren(4).create();
    while (peakelsIt.hasNext()) {
      Peakel peakel = peakelsIt.next();
      peakelsCount++;
      entries.add(Entries.entry(peakel.getId(), Geometries.point(peakel.getMz(), peakel.getElutionTime())));
    }
    LOG.info("read {} peakels from peakelDb {}", peakelsCount, peakelDbFile.getAbsolutePath());
    rTree = rTree.add(entries);
    LOG.info("rTree completed with " + rTree.size() + " entries");


    //
    // Mimic PeakelsDetector._searchForUnidentifiedFeatures
    //

    Metric metrics = new Metric("PeakelsDbFinder");

    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
    String[] columns = {"id", "mz", "charge", "rt", "intensity", "peakel_elution_time", "peakel_count", "peakel_ids", "base_peakel_idx, isReliable"};

    writer.write(Arrays.stream(columns).collect(Collectors.joining(DELIMITER)));
    writer.newLine();

    LOG.info("Searching for {} putative ions/features", putativeFts.size());

    for (PutativeFeature putativeFt : putativeFts) {


      final Peakel[] peakels = PeakelDbHelper$.MODULE$.findPeakelsInRange(
              connection,
              Option$.MODULE$.apply(rTree),
              putativeFt.mz() - ISOTOPE_PATTERN_HALF_MZ_WINDOW,
              putativeFt.mz() + ISOTOPE_PATTERN_HALF_MZ_WINDOW,
              putativeFt.elutionTime() - 40,
              putativeFt.elutionTime() + 40);

      List<Peakel> coelutingPeakels = Arrays.asList(peakels);

      try {


        final Option<Tuple2<Peakel, Object>> matchingPeakel = PeakelDbHelper$.MODULE$.findMatchingPeakel(
                (Seq<Peakel>) JavaConverters.collectionAsScalaIterableConverter(coelutingPeakels).asScala().toSeq(),
                putativeFt,
                mzTolPPM,
                mzTolPPM,
                Option$.MODULE$.apply(assignedPeakelIds),
                Option$.MODULE$.apply(null),
                metrics);

        if (matchingPeakel.isDefined()) {
          Peakel peakel = matchingPeakel.get()._1;
          Boolean isReliable = (Boolean) matchingPeakel.get()._2;

          final Peakel[] isotopes = PeakelDbHelper$.MODULE$.findFeatureIsotopes(connection, Option.apply(rTree), peakel, putativeFt.charge(), mzTolPPM);
          Tuple2<Peakel, Object>[] indexedPeakels = new Tuple2[isotopes.length];
          for (int k = 0; k < isotopes.length; k++) {
            indexedPeakels[k] = new Tuple2<>(isotopes[k], k);
          }

          Feature feature = new FeatureWrapper(Feature.generateNewId(), peakel.getMz(), putativeFt.charge(), indexedPeakels, false, new long[0]);

          StringBuilder strBuilder = new StringBuilder();
          strBuilder.append(putativeFt.id()).append(DELIMITER);
          strBuilder.append(feature.getMz()).append(DELIMITER);
          strBuilder.append(putativeFt.charge()).append(DELIMITER);
          strBuilder.append(putativeFt.elutionTime()).append(DELIMITER);
          strBuilder.append(feature.getBasePeakel().getApexIntensity()).append(DELIMITER);
          strBuilder.append(feature.getBasePeakel().getElutionTime()).append(DELIMITER);
          strBuilder.append(feature.getPeakels().length).append(DELIMITER);
          strBuilder.append(Arrays.stream(feature.getPeakels()).map(p -> Integer.toString(p.getId())).collect(Collectors.joining(", ", "{", "}"))).append(DELIMITER);
          strBuilder.append(feature.getBasePeakelIndex()).append(DELIMITER);
          strBuilder.append(isReliable);

          writer.write(strBuilder.toString());
          writer.newLine();
          writer.flush();
          metrics.incr("putative_ion_found");
//          LOG.info("Feature found for putative Ft ({} , {}) = {}, reliable = {} ",putativeFt.mz(), putativeFt.charge(), DF.format(feature.getBasePeakel().getApexIntensity()), isReliable);
        } else {
          metrics.incr("putative_ion_not_found");
          LOG.info("no Feature found for putative Ft ({} , {})", putativeFt.mz(), putativeFt.charge());
        }

      } catch (Exception e) {
        LOG.info("error ", e);
      }
    }

    LOG.info("Metrics : {}", metrics.toString());
  }

}
