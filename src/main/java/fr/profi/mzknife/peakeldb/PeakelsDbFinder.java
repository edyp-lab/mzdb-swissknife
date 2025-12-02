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
import fr.profi.mzdb.db.model.params.param.CVEntry;
import fr.profi.mzdb.featuredb.FeatureDbReader;
import fr.profi.mzdb.model.Feature;
import fr.profi.mzdb.model.LcMsPeak;
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
  private static final int ISOTOPE_PATTERN_HALF_MZ_WINDOW = 5;

  private final File peakelDbFile;
  private final RTree<Integer, Point> rTree;
  private final SQLiteConnection connection;


  public PeakelsDbFinder(File peakelDbFile) {
    try {

      this.peakelDbFile = peakelDbFile;
      connection = new SQLiteConnection(peakelDbFile);
      connection.openReadonly();

      Iterator<Peakel> peakelsIt = asJavaIterable(PeakelDbReader.loadAllPeakels(connection, 800000)).iterator();
      rTree = buildRTree(peakelsIt);

    } catch (SQLiteException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Matches a list of identified PSMs (Peptide-Spectrum Matches) to experimental features
   * by screening peakels from a database. It identifies corresponding features for each PSM
   * within a specified m/z tolerance and optionally groups PSMs by their ion keys.
   *
   * @param psms the list of putative feature wrappers representing the PSMs to be matched
   * @param reader the MzDbReader used to access spectrum data for matching
   * @param mzTolPPM the mass-to-charge ratio tolerance in parts per million for matching PSMs to peakels
   * @param groupPSMs whether to group the PSMs by their ion keys after matching
   * @return a list of putative feature wrappers, optionally grouped, with added experimental features matching the PSMs
   * @throws Exception if an error occurs during matching or interacting with the database
   */
  public List<PutativeFeatureWrapper> matchIdentifiedPsms(List<PutativeFeatureWrapper> psms, MzDbReader reader, Float mzTolPPM, Boolean groupPSMs) throws Exception {

    Metric metrics = new Metric("PeakelsDbFinder");
    LOG.info("Indexing {} identified PSMs by Cycle", psms.size());
    Map<Integer, List<PutativeFeatureWrapper>> psmsByCycle = new HashMap<>();

    for (PutativeFeatureWrapper psm : psms) {

      try {
        int cycle = (psm.elutionTime() > 0) ? reader.getSpectrumHeaderForTime(psm.elutionTime(), 2).getCycle() : reader.getSpectrumHeader(psm.getSpectrumId()).getCycle();
        psmsByCycle.computeIfAbsent(cycle, k -> new ArrayList<>()).add(psm);
      } catch (NullPointerException npe) {
        LOG.warn("PSM {}, {} : retention time or scan number not found", psm.getId(), psm.getIonKey());
      }
    }

    LOG.info("Searching for {} identified PSMs", psms.size());
    long start = System.currentTimeMillis();

    Set<PutativeFeatureWrapper> matchedPsms = new HashSet<>();

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
//            matchedPsms.add(psm.id());
            matchedPsms.add(psm);
            metrics.incr("matching_peakels_found");
          }
        }
        currentCycle++;
      }
      if ((peakelsCount % 50000) == 0) LOG.info("scanned peakels : {}", peakelsCount);
    }

    LOG.info("Total Duration : {} ms to screen {} peakels", (System.currentTimeMillis() - start),  peakelsCount);

    LOG.info("Total number of matched psms {} / {} submitted",matchedPsms.size(), psms.size());
    LOG.info("Total number of matched ions {} / {} submitted",matchedPsms.stream().map(p -> p.getIonKey()).distinct().count(), psms.stream().map(p -> p.getIonKey()).distinct().count());
    LOG.info("Total number of matched peptides {} / {} submitted", matchedPsms.stream().map(p -> p.getPeptideKey()).distinct().count(), psms.stream().map(p -> p.getPeptideKey()).distinct().count());

    if (groupPSMs) {
      return FeaturesHelper.groupByIonKeys(psms);
    }

    return psms;
  }



  /**
   * Matches putative features with experimental peakels from a database within a specified m/z tolerance.
   * This method attempts to find corresponding experimental features for each input putative feature.
   *
   * @param putativeFts the list of putative feature wrappers to be matched with experimental features
   * @param assignedPeakelsFile a file containing the list of peakel IDs that are assigned and excluded from matching
   * @param mzTolPPM the mass-to-charge (m/z) tolerance in parts per million for matching putative features to peakels
   * @return a list of putative feature wrappers with associated experimental features if matches are found
   * @throws IOException if an error occurs while reading the assigned peakels file
   */
  public List<PutativeFeatureWrapper> matchPutativeFeatures(List<PutativeFeatureWrapper> putativeFts, File assignedPeakelsFile, Float mzTolPPM) throws IOException {

    //
    // Build assigned peakels list
    //
    final scala.collection.immutable.Set<Object> assignedPeakelIds = getAssignedPeakelIds(assignedPeakelsFile);

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
              putativeFt.elutionTime() - Math.max(putativeFt.elutionTimeTolerance(), 60),
              putativeFt.elutionTime() + Math.max(putativeFt.elutionTimeTolerance(), 60));

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
    return putativeFts;
  }

  private static scala.collection.immutable.Set<Object> getAssignedPeakelIds(File assignedPeakelsFile) throws IOException {
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
    return assignedPeakelIds;
  }


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
