package fr.profi.mzknife.peakeldb;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.github.davidmoten.rtree.Entries;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.featuredb.FeatureDbReader;
import fr.profi.mzdb.model.Feature;
import fr.profi.mzdb.model.Peakel;
import fr.profi.mzdb.model.PutativeFeature;
import fr.profi.mzdb.peakeldb.PeakelDbHelper$;
import fr.profi.mzdb.peakeldb.io.PeakelDbReader;
import fr.profi.mzknife.PeakelsProcessing;
import fr.profi.util.metrics.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Option$;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static scala.collection.JavaConversions.asJavaIterable;

public class PeakelsDbFinder {

  private final static Logger LOG = LoggerFactory.getLogger(PeakelsDbFinder.class);
  private static final CharSequence DELIMITER = ";";
  private static final DecimalFormat DF = new DecimalFormat("#");
  private static final int ISOTOPE_PATTERN_HALF_MZ_WINDOW = 5;

  private final File outputFile;
  private final File peakelDbFile;
  private final RTree<Integer, Point> rTree;
  private SQLiteConnection connection;


  public PeakelsDbFinder(File peakelDbFile, File outputFile) {
    try {

      this.outputFile = outputFile;
      this.peakelDbFile = peakelDbFile;
      connection = new SQLiteConnection(peakelDbFile);
      connection.openReadonly();

      Iterator<Peakel> peakelsIt = asJavaIterable(PeakelDbReader.loadAllPeakels(connection, 800000)).iterator();
      rTree = buildRTree(peakelsIt);

    } catch (SQLiteException e) {
      throw new RuntimeException(e);
    }
  }

  public void matchIdentifiedPsms(PeakelsProcessing.InputSource inputSource, MzDbReader reader, Float mzTolPPM, Boolean groupPSMs) throws Exception {

    Metric metrics = new Metric("PeakelsDbFinder");

    final List<PutativeFeatureWrapper> psms = inputSource.getPutativeFeatures();
    LOG.info("Indexing {} identified PSMs by Cycle", psms.size());

    Map<Integer, List<PutativeFeatureWrapper>> psmsByCycle = new HashMap<>();
    // for optimization : use an array instead of a map ?
    //    List<PutativeFeature>[] psmsByCycle = new List[reader.getCyclesCount()+1];

    for (PutativeFeatureWrapper psm : psms) {
      int cycle = reader.getSpectrumHeaderForTime(psm.elutionTime(), 2).getCycle();
      psmsByCycle.computeIfAbsent(cycle, k -> new ArrayList<>()).add(psm);
    }

    LOG.info("Searching for {} identified PSMs", psms.size());
    long start = System.currentTimeMillis();

    Set<Integer> matchedPsms = new HashSet<>();
    Set<PutativeFeatureWrapper> matchedPsms2 = new HashSet<>();

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
        final List<PutativeFeatureWrapper> psmsList = psmsByCycle.get(currentCycle);;
        if (psmsList != null) {
          final List<PutativeFeatureWrapper> matchingPsms = psmsList.stream().filter(f -> Math.abs(f.getMz() - peakel.getMz()) <= mzTolDa).collect(Collectors.toUnmodifiableList());

          for (PutativeFeatureWrapper psm : matchingPsms) {
            final Feature feature = buildFeature(peakel, psm, mzTolPPM);
            psm.addExperimentalFeature(feature);
            matchedPsms.add(psm.id());
            matchedPsms2.add(psm);
            metrics.incr("matching_peakels_found");
          }
        }
        currentCycle++;
      }
      if ((peakelsCount % 50000) == 0) LOG.info("scanned peakels : {}", peakelsCount);
    }

    LOG.info("Total Duration : {} ms to match {} psms (over {}) to {} peakels", (System.currentTimeMillis() - start), matchedPsms.size(), psms.size(), peakelsCount);

    LOG.info("Total number of ions {}", psms.stream().map(p -> p.getKey()).distinct().count());
    LOG.info("Total number of peptides {}", psms.stream().map(p -> p.getPeptideKey()).distinct().count());

    LOG.info("Total number of matched ions {}", matchedPsms2.stream().map(p -> p.getKey()).distinct().count());
    LOG.info("Total number of matched peptides {}", matchedPsms2.stream().map(p -> p.getPeptideKey()).distinct().count());

    if (groupPSMs) {
      long startGrouping = System.currentTimeMillis();
      final Map<String, List<PutativeFeatureWrapper>> psmsByKey = psms.stream().collect(Collectors.groupingBy(p -> p.getKey()));

      final List<PutativeFeatureWrapper> ions = new ArrayList<>(psmsByKey.size());
      final Map<Integer, List<Integer>> ions2PeakelIds = new HashMap<>();

      for (Map.Entry<String, List<PutativeFeatureWrapper>> e : psmsByKey.entrySet()) {
        final List<PutativeFeatureWrapper> putativeFeatures = e.getValue();
        final List<PutativeFeatureWrapper> matchedPutativefeatures = putativeFeatures.stream().filter(ft -> !ft.getExperimentalFeatures().isEmpty()).collect(Collectors.toList());
//        final Optional<PutativeFeatureWrapper> representativeFt = matchedPutativefeatures.stream().max(Comparator.comparing(ft -> {
//          return ft.getExperimentalFeatures().stream().max(Comparator.comparing(ef -> ef.getBasePeakel().getApexIntensity())).get().getBasePeakel().getApexIntensity();
//        }));
        final Optional<PutativeFeatureWrapper> representativeFt = matchedPutativefeatures.stream().max(Comparator.comparing(ft -> ft.getRepresentativeExperimentalFeature().getBasePeakel().getApexIntensity()));
        if (representativeFt.isPresent()) {
//          final Set<Integer> peakelIds = matchedPutativefeatures.stream().flatMap(pf -> pf.getExperimentalFeatures().stream().flatMap(f -> Arrays.stream(f.getPeakels()).map(peakel -> peakel.getId()))).collect(Collectors.toSet());
          final List<Integer> peakelIds = matchedPutativefeatures.stream().flatMap(pf -> pf.getExperimentalFeatures().stream().flatMap(f -> Arrays.stream(f.getPeakels()).map(peakel -> peakel.getId()))).distinct().toList();

          ions.add(representativeFt.get());
          ions2PeakelIds.put(representativeFt.get().id(), peakelIds);
        }
      }
      LOG.info("Grouping Duration : {} ms to generate {} ions", (System.currentTimeMillis() - startGrouping), ions.size());
      writeFeatures(ions, inputSource.getOriginalLines(), inputSource.getOriginalHeader(), ions2PeakelIds);
    } else {
      writeFeatures(psms, inputSource.getOriginalLines(), inputSource.getOriginalHeader(), null);
    }

    LOG.info("Metrics : {}", metrics);
  }



  public void matchPutativeFeatures(PeakelsProcessing.InputSource inputSource, File assignedPeakelsFile, Float mzTolPPM) throws IOException {

    List<PutativeFeatureWrapper> putativeFts = inputSource.getPutativeFeatures();

    //
    // Build assigned peakels list
    //
    scala.collection.immutable.Set<Object> assignedPeakelIds = null;
    try {
      if (assignedPeakelsFile != null) {
        if (assignedPeakelsFile.getName().endsWith(".sqlite")) {
          SQLiteConnection ft_connection = new SQLiteConnection(assignedPeakelsFile);
          ft_connection.openReadonly();
          assignedPeakelIds = FeatureDbReader.loadAssignedPeakelIds(ft_connection);
          ft_connection.dispose();
        } else if (assignedPeakelsFile.getName().endsWith(".csv")) {
          try {
            Set<Integer> idsSet = new HashSet<>();
            final CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
            CSVReader reader = new CSVReaderBuilder(new FileReader(assignedPeakelsFile)).withCSVParser(parser).build();
            String[] columns = reader.readNext();
            final int index = IntStream.range(0, columns.length).filter(i -> columns[i].equalsIgnoreCase("cluster.peakels.ids")).findFirst().orElse(-1);
            if (index >= 0) {
              String[] split;
              while ((split = reader.readNext()) != null) {
                  String peakelIdsStr = split[index];
                peakelIdsStr = peakelIdsStr.substring(1, peakelIdsStr.length()-1);
                final List<Integer> ids = Arrays.stream(peakelIdsStr.split(",")).map(s -> Integer.valueOf(s.trim())).collect(Collectors.toList());
                idsSet.addAll(ids);
              }

              assignedPeakelIds = JavaConverters.asScalaSet(idsSet).toSet();
            }
          } catch (CsvValidationException cve) {
            LOG.error("assigned peakels CSV format is invalid", cve);
          }
        }

        LOG.info("Assigned peakels list size = "+assignedPeakelIds.size());

      }

    } catch (SQLiteException e) {
      LOG.error("Error while reading featureDb file " + assignedPeakelsFile.getAbsolutePath(), e);
    }


    //
    // Mimic PeakelsDetector._searchForUnidentifiedFeatures
    //
    LOG.info("start reading peakels ...");
    Metric metrics = new Metric("PeakelsDbFinder");
    LOG.info("Searching for {} putative ions/features", putativeFts.size());
    long start = System.currentTimeMillis();
    int  peakelsCount = 0;
    for (PutativeFeatureWrapper putativeFt : putativeFts) {

      final Peakel[] peakels = PeakelDbHelper$.MODULE$.findPeakelsInRange(
              connection,
              Option$.MODULE$.apply(rTree),
              putativeFt.mz() - ISOTOPE_PATTERN_HALF_MZ_WINDOW,
              putativeFt.mz() + ISOTOPE_PATTERN_HALF_MZ_WINDOW,
              putativeFt.elutionTime() - putativeFt.elutionTimeTolerance(),
              putativeFt.elutionTime() + putativeFt.elutionTimeTolerance());

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
          peakelsCount++;
          Peakel peakel = matchingPeakel.get()._1;
          Boolean isReliable = (Boolean) matchingPeakel.get()._2;

          final Feature feature = buildFeature(peakel, putativeFt, mzTolPPM);
          putativeFt.addExperimentalFeature(feature);
          putativeFt.setIsReliable(isReliable);
          metrics.incr("putative_ion_found");

        } else {
          metrics.incr("putative_ion_not_found");
          LOG.info("no Feature found for putative Ft ({} , {})", putativeFt.mz(), putativeFt.charge());
        }

      } catch (Exception e) {
        LOG.info("error ", e);
      }
    }

    LOG.info("Total Duration : {} ms to match {} peakels ", (System.currentTimeMillis() - start), peakelsCount);

    writeFeatures(putativeFts, inputSource.getOriginalLines(), inputSource.getOriginalHeader(), null);

    LOG.info("Metrics : {}", metrics);
  }

  private void writeFeatures(List<PutativeFeatureWrapper> putativeFeatures, Map<Integer, String> originalLines, String originalHeader, Map<Integer, List<Integer>> ions2peakels) throws IOException {

    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
    String[] columns = {"ion.id", "ion.mz", "ion.charge", "ion.rt", "ft.intensity", "ft.elution_time", "ft.mz", "peakels.count", "peakel.ids", "ft.base_peakel_idx", "ft.isReliable", "cluster.peakels.ids"};

    writer.write(Arrays.stream(columns).collect(Collectors.joining(DELIMITER)));
    writer.write(DELIMITER+originalHeader);
    writer.newLine();

    for (PutativeFeatureWrapper putativeFt : putativeFeatures) {
        Feature feature = putativeFt.getRepresentativeExperimentalFeature();
        if (feature != null) {
          StringBuilder strBuilder = new StringBuilder();
          strBuilder.append(putativeFt.id()).append(DELIMITER);
          strBuilder.append(putativeFt.getMz()).append(DELIMITER);
          strBuilder.append(putativeFt.charge()).append(DELIMITER);
          strBuilder.append(putativeFt.elutionTime()).append(DELIMITER);
          strBuilder.append(feature.getBasePeakel().getApexIntensity()).append(DELIMITER);
//          strBuilder.append(feature.getBasePeakel().getElutionTime()).append(DELIMITER);
          strBuilder.append(feature.getElutionTime()).append(DELIMITER);
          strBuilder.append(feature.getMz()).append(DELIMITER);
          strBuilder.append(feature.getPeakels().length).append(DELIMITER);
          strBuilder.append(Arrays.stream(feature.getPeakels()).map(p -> Integer.toString(p.getId())).collect(Collectors.joining(", ", "{", "}"))).append(DELIMITER);
          strBuilder.append(feature.getBasePeakelIndex());
          if (putativeFt.isReliable().isPresent()) {
            strBuilder.append(DELIMITER).append(putativeFt.isReliable().get()).append(DELIMITER);
          } else {
            strBuilder.append(DELIMITER).append("").append(DELIMITER);
          }
          if (ions2peakels != null) {
            strBuilder.append(ions2peakels.get(putativeFt.id()).stream().map(i -> Integer.toString(i)).collect(Collectors.joining(", ", "{", "}")));
          } else {
            strBuilder.append("");
          }
          final String s = originalLines.get(putativeFt.id());
          if (s != null) {
            strBuilder.append(DELIMITER).append(s);
          }
          writer.write(strBuilder.toString());
          writer.newLine();
        }
    }
    writer.flush();
    writer.close();
  }


//  private static void writeFeature(PutativeFeature putativeFt, Feature feature, BufferedWriter writer, Map<Integer, String> originalLines, Optional<Boolean> isReliable) throws IOException {
//    StringBuilder strBuilder = new StringBuilder();
//    strBuilder.append(putativeFt.id()).append(DELIMITER);
//    strBuilder.append(putativeFt.getMz()).append(DELIMITER);
//    strBuilder.append(putativeFt.charge()).append(DELIMITER);
//    strBuilder.append(putativeFt.elutionTime()).append(DELIMITER);
//    strBuilder.append(feature.getBasePeakel().getApexIntensity()).append(DELIMITER);
////          strBuilder.append(feature.getBasePeakel().getElutionTime()).append(DELIMITER);
//    strBuilder.append(feature.getElutionTime()).append(DELIMITER);
//    strBuilder.append(feature.getMz()).append(DELIMITER);
//    strBuilder.append(feature.getPeakels().length).append(DELIMITER);
//    strBuilder.append(Arrays.stream(feature.getPeakels()).map(p -> Integer.toString(p.getId())).collect(Collectors.joining(", ", "{", "}"))).append(DELIMITER);
//    strBuilder.append(feature.getBasePeakelIndex());
//    if (isReliable.isPresent()) {
//      strBuilder.append(DELIMITER).append(isReliable);
//    }
//
//    final String s = originalLines.get(putativeFt.id());
//    if (s != null) {
//      strBuilder.append(DELIMITER).append(s);
//    }
//
//    writer.write(strBuilder.toString());
//    writer.newLine();
//    writer.flush();
//  }

  private Feature buildFeature(Peakel peakel, PutativeFeature putativeFt, Float mzTolPPM) {
    final Peakel[] isotopes = PeakelDbHelper$.MODULE$.findFeatureIsotopes(connection, Option.apply(rTree), peakel, putativeFt.charge(), mzTolPPM);
    Tuple2<Peakel, Object>[] indexedPeakels = new Tuple2[isotopes.length];
    for (int k = 0; k < isotopes.length; k++) {
      indexedPeakels[k] = new Tuple2<>(isotopes[k], k);
    }

    Feature feature = new FeatureAdapter(Feature.generateNewId(), peakel.getMz(), putativeFt.charge(), indexedPeakels, false, new long[0]);
    return feature;
  }

  private RTree<Integer, Point> buildRTree(Iterator<Peakel> peakelsIt) {
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
    return rTree;
  }

}
