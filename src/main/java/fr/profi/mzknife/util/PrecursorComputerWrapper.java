package fr.profi.mzknife.util;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.writer.mgf.IPrecursorComputation;
import fr.profi.mzdb.io.writer.mgf.MgfPrecursor;
import fr.profi.mzdb.model.SpectrumHeader;
import org.apache.commons.lang3.ArrayUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class PrecursorComputerWrapper implements IPrecursorComputation {

  private static final CharSequence DELIMITER = "\t";
  private final BufferedWriter writer;
  private IPrecursorComputation wrappedPrecComputer;

  private String[] precAnnotations = { "mz", "charge", "precursors.count.sw" };
  private String[] annotations = {"source", "mgf.id", "scan.number",  "in.sw", "precursor.intensity.sw", "prediction", "prediction.pattern.score", "rank",  "filtered.peaks.count.sw", "precursor.signal.total.sw", "precursor.signal.max.sw", "precursor.rank.sw" };
  public PrecursorComputerWrapper(IPrecursorComputation wrappedPrecComputer, BufferedWriter annotationWriter) {
    this.wrappedPrecComputer = wrappedPrecComputer;
    this.writer = annotationWriter;
  }

  @Override
  public String getMethodName() {
    return wrappedPrecComputer.getMethodName();
  }

  @Override
  public String getMethodVersion() { return wrappedPrecComputer.getMethodVersion(); }

  @Override
  public MgfPrecursor[] getMgfPrecursors(MzDbReader mzDbReader, SpectrumHeader spectrumHeader) throws SQLiteException {

    final MgfPrecursor[] precursors = wrappedPrecComputer.getMgfPrecursors(mzDbReader, spectrumHeader);
    for(MgfPrecursor precursor : precursors) {
        try {
          final String annotationsAsString = Arrays.stream(annotations).map(k -> precursor.getAnnotationOrElse(k, "").toString()).collect(Collectors.joining(DELIMITER));
          StringBuilder strBuilder = new StringBuilder();
          strBuilder.append(precursor.getPrecMz()).append(DELIMITER);
          strBuilder.append(precursor.getCharge()).append(DELIMITER);
          strBuilder.append(precursors.length).append(DELIMITER).append(annotationsAsString);
          writer.write(strBuilder.toString());
          writer.newLine();
          writer.flush();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
    }

    return precursors;
  }

  public String[] getAnnotations() {
    return ArrayUtils.addAll(precAnnotations, annotations);
  }

  public CharSequence getDelimiter() {
    return DELIMITER;
  }
}
