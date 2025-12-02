package fr.profi.mzknife.peakeldb;

import fr.profi.mzdb.model.Feature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeatureWriter {

  private final static Logger LOG = LoggerFactory.getLogger(FeatureWriter.class);

  public static final String[] FT_COLUMNS = new String[]{"ion.id", "ion.mz", "ion.charge", "ion.rt", "ft.intensity", "ft.area", "ft.elution_time", "ft.mz", "peakels.count", "peakel.ids", "ft.base_peakel_idx", "ft.isReliable", "cluster.peakels.ids", "ft.faims.cv", "ft.rawfile"};

  public static void writeFeatures(File outputFile,
                                   List<PutativeFeatureWrapper> putativeFeatures,
                                   CharSequence delimiter,
                                   Map<Integer, String> originalLines,
                                   String originalHeader,
                                   boolean outputUnassigned) throws IOException {

    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

    writer.write(String.join(delimiter, FT_COLUMNS));
    writer.write(delimiter+originalHeader);
    writer.newLine();

    for (PutativeFeatureWrapper putativeFt : putativeFeatures) {
      StringBuilder strBuilder = new StringBuilder();

      if (putativeFt.isMatched() || outputUnassigned) {
        writeMatchedFeature(putativeFt, strBuilder, delimiter);
        final String s = originalLines.get(putativeFt.id());
        if (s != null) {
          strBuilder.append(s);
        }
      }

      if (!strBuilder.isEmpty()) {
        writer.write(strBuilder.toString());
        writer.newLine();
      }

    }
    writer.flush();
    writer.close();
  }

  private static void writeMatchedFeature(PutativeFeatureWrapper putativeFt, StringBuilder strBuilder, CharSequence delimiter) {


    final String listSeparator = delimiter.equals(",") ? "; " : ", ";

    strBuilder.append(putativeFt.id()).append(delimiter);
    strBuilder.append(putativeFt.getMz()).append(delimiter);
    strBuilder.append(putativeFt.charge()).append(delimiter);
    strBuilder.append(putativeFt.elutionTime()).append(delimiter);

    if (putativeFt.isMatched()) {
      Feature feature = putativeFt.getRepresentativeExperimentalFeature();
      strBuilder.append(feature.getBasePeakel().getApexIntensity()).append(delimiter);
      strBuilder.append(feature.getBasePeakel().getArea()).append(delimiter);
      strBuilder.append(feature.getElutionTime()).append(delimiter);
      strBuilder.append(feature.getMz()).append(delimiter);
      strBuilder.append(feature.getPeakels().length).append(delimiter);
      // isotopes peakel ids of all matching experimental peakels
      strBuilder.append(Arrays.stream(feature.getPeakels()).map(p -> Integer.toString(p.getId())).collect(Collectors.joining(listSeparator, "{", "}"))).append(delimiter);
      strBuilder.append(feature.getBasePeakelIndex()).append(delimiter);
      strBuilder.append(putativeFt.isReliable().isPresent() ? putativeFt.isReliable().get(): "").append(delimiter);
      // isotopes peakel ids of all matching experimental peakels of all grouped features
      if (putativeFt.getGroupedFeatures() != null) {
        strBuilder.append(putativeFt.getGroupedPeakelIds().stream().map(i -> Integer.toString(i)).collect(Collectors.joining(listSeparator, "{", "}"))).append(delimiter);
      } else {
        strBuilder.append(delimiter);
      }

    } else {
      // no experimental feature found, output empty columns except for columns [ID, MZ, CHARGE, RT, CV, RAW]
      strBuilder.append(String.valueOf(delimiter).repeat(FT_COLUMNS.length - 6));
    }

    strBuilder.append(putativeFt.getCvValue() != null ? Float.valueOf(putativeFt.getCvValue()).intValue() : "").append(delimiter);
    strBuilder.append(putativeFt.getRawSourceFile() != null ? putativeFt.getRawSourceFile() : "").append(delimiter);
  }

  public static void writeIons(File outputFile,
                               List<ConsensusIon> consensusIons,
                               CharSequence delimiter,
                               Map<Integer, String> originalLines,
                               String originalHeader,
                               boolean outputUnassigned,
                               List<String> allRuns) throws IOException {

    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
    String[] columns = buildColumns(allRuns);

    writer.write(String.join(delimiter, columns));
    writer.write(delimiter + originalHeader);
    writer.newLine();

    for (ConsensusIon consensusIon : consensusIons) {
      StringBuilder strBuilder = new StringBuilder();
      for (String run : allRuns) {
        final List<PutativeFeatureWrapper> putativeFeatures = consensusIon.getPutativeFeatures(run);

        if (putativeFeatures != null) {
          if (putativeFeatures.size() == 1) {
            // TODO : select the "best feature" instead of the first one
            final PutativeFeatureWrapper putativeFt = putativeFeatures.get(0);
            if (putativeFt.isMatched() || outputUnassigned) {
              writeMatchedFeature(putativeFt, strBuilder, delimiter);
            }
          } else {
            LOG.error("!!!! More than one Putative Ft found for run {}", run);
          }
        } else {
          strBuilder.append(consensusIon.getRepresentativeFeature().id()).append(delimiter);
          strBuilder.append(String.valueOf(delimiter).repeat(FT_COLUMNS.length - 1));
        }
      }

      final String s = originalLines.get(consensusIon.representativeFeature.id());
      if (s != null) {
        strBuilder.append(s);
      }


      if (!strBuilder.isEmpty()) {
        writer.write(strBuilder.toString());
        writer.newLine();
      }
    }
    writer.flush();
    writer.close();
  }

  private static String[] buildColumns(List<String> allRuns) {
    List<String> columns = new ArrayList<>(FT_COLUMNS.length*allRuns.size());
    for (String run : allRuns) {
      columns.addAll(Arrays.stream(FT_COLUMNS).map(c -> run+"."+c).toList());
    }
    return columns.toArray(new String[0]);
  }

}
