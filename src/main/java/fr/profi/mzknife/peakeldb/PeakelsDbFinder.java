package fr.profi.mzknife.peakeldb;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.github.davidmoten.rtree.Entries;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;
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
import scala.collection.Seq;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static scala.collection.JavaConversions.asJavaIterable;

public class PeakelsDbFinder {

  private final static Logger LOG = LoggerFactory.getLogger(PeakelsDbFinder.class);
  private static final CharSequence DELIMITER = "\t";
  private static final DecimalFormat DF = new DecimalFormat("#");

  private final File m_dstFile;
  private RTree<Integer, Point> rTree;
  private SQLiteConnection connection;


  public PeakelsDbFinder(File peakelDbFile, File outputFile) {

    try {

      this.m_dstFile = outputFile;

      connection = new SQLiteConnection(peakelDbFile);
      connection.openReadonly();

      LOG.info("start reading peakels ...");

      final Seq<Object> ids = PeakelDbReader.findPeakelIdsInRangeFromPeakelDB(connection, 800.0, 850.0, 0, 20 * 60);

      Iterator<Peakel> peakelsIt = asJavaIterable(PeakelDbReader.loadAllPeakels(connection, 800000)).iterator();
      List<Entry<Integer, Point>> entries = new ArrayList<Entry<Integer, Point>>();

      rTree = RTree.star().maxChildren(4).create();
      while (peakelsIt.hasNext()) {
        Peakel peakel = peakelsIt.next();
        entries.add(Entries.entry(peakel.getId(), Geometries.point(peakel.getMz(), peakel.getElutionTime())));
      }

      rTree = rTree.add(entries);
      LOG.info("rTree completed with " + rTree.size() + " entries");

    } catch (SQLiteException e) {
      throw new RuntimeException(e);
    }
  }

  public void findPeakels(List<PutativeFeature> putativeFts, Float mzTolPPM) throws IOException {

    Metric metric = new Metric("PeakelsDbFinder");

    BufferedWriter writer = new BufferedWriter(new FileWriter(m_dstFile));
    String[] columns = { "id", "mz", "charge", "intensity", "elution_time", "peakel_count", "peakel_ids", "base_peakel_idx"};

    writer.write(Arrays.stream(columns).collect(Collectors.joining(DELIMITER)));
    writer.newLine();

    for (PutativeFeature putativeFt : putativeFts) {


      final Peakel[] peakels = PeakelDbHelper$.MODULE$.findPeakelsInRange(
              connection,
              Option$.MODULE$.apply(rTree),
              putativeFt.mz() - 5,
              putativeFt.mz() + 5,
              putativeFt.elutionTime() - 40,
              putativeFt.elutionTime() + 40);

      List<Peakel> coelutingPeakels = Arrays.asList(peakels);

      try {
        final Option<Tuple2<Peakel, Object>> matchingPeakel = PeakelDbHelper$.MODULE$.findMatchingPeakel(
                JavaConverters.collectionAsScalaIterableConverter(coelutingPeakels).asScala().toSeq(),
                putativeFt,
                mzTolPPM,
                mzTolPPM,
                Option$.MODULE$.apply(null),
                Option$.MODULE$.apply(null),
                metric);

        if (matchingPeakel.isDefined()) {
           Peakel peakel = matchingPeakel.get()._1;
           Boolean isReliable = (Boolean)matchingPeakel.get()._2;

          final Peakel[] isotopes = PeakelDbHelper$.MODULE$.findFeatureIsotopes(connection, Option.apply(rTree), peakel, putativeFt.charge(), mzTolPPM);
          Tuple2<Peakel, Object>[] indexedPeakels = new Tuple2[isotopes.length];
          for(int k = 0; k < isotopes.length; k++) {
            indexedPeakels[k] = new Tuple2<>(isotopes[k], k);
          }

          Feature feature = new Feature(Feature.generateNewId(), peakel.getMz(), putativeFt.charge(), indexedPeakels, false, new long[0]);

          StringBuilder strBuilder = new StringBuilder();
          strBuilder.append(putativeFt.id()).append(DELIMITER);
          strBuilder.append(peakel.getMz()).append(DELIMITER);
          strBuilder.append(putativeFt.charge()).append(DELIMITER);
          strBuilder.append(feature.getBasePeakel().getApexIntensity()).append(DELIMITER);
          strBuilder.append(feature.getBasePeakel().getElutionTime()).append(DELIMITER);
          strBuilder.append(feature.getPeakels().length).append(DELIMITER);
          strBuilder.append(Arrays.stream(feature.getPeakels()).map(p -> Integer.toString(p.getId())).collect(Collectors.joining(", ", "{", "}"))).append(DELIMITER);
          strBuilder.append(feature.getBasePeakelIndex());

          writer.write(strBuilder.toString());
          writer.newLine();
          writer.flush();

          LOG.info("Feature found for putative Ft ({} , {}) = {} ",putativeFt.mz(), putativeFt.charge(), DF.format(feature.getBasePeakel().getApexIntensity()));
        } else {
          LOG.info("no Feature found for putative Ft ({} , {}) = ",putativeFt.mz(), putativeFt.charge());
        }

      } catch (Exception e) {
        LOG.info("error ",e);
      }
    }

  }

}
