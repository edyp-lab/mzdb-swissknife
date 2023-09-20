package fr.profi.mzknife.util;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.writer.mgf.AnnotatedMgfPrecursor;
import fr.profi.mzdb.io.writer.mgf.IPrecursorComputation;
import fr.profi.mzdb.io.writer.mgf.MgfPrecursor;
import fr.profi.mzdb.model.SpectrumHeader;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class PrecursorComputerWrapper implements IPrecursorComputation {

  private final BufferedWriter writer;
  private IPrecursorComputation wrappedPrecComputer;

  private String[] annotations = {"source", "scan.number", "mz", "charge", "in.sw", "precursors.count.sw", "precursor.intensity.sw", "prediction", "rank",  "maxPeak", "filtered.peaks.count.sw", "precursor.signal.total.sw", "precursor.signal.max.sw", "precursor.rank.sw" };
  public PrecursorComputerWrapper(IPrecursorComputation wrappedPrecComputer, BufferedWriter annotationWriter) {
    this.wrappedPrecComputer = wrappedPrecComputer;
    this.writer = annotationWriter;
  }

  @Override
  public String getMethodName() {
    return wrappedPrecComputer.getMethodName();
  }

  @Override
  public MgfPrecursor[] getMgfPrecursors(MzDbReader mzDbReader, SpectrumHeader spectrumHeader) throws SQLiteException {

    final MgfPrecursor[] precursors = wrappedPrecComputer.getMgfPrecursors(mzDbReader, spectrumHeader);
    for(MgfPrecursor precursor : precursors) {
      if (AnnotatedMgfPrecursor.class.isAssignableFrom(precursor.getClass())) {
        AnnotatedMgfPrecursor annotatedPrecursor = (AnnotatedMgfPrecursor) precursor;
        try {
          writer.write(Arrays.stream(annotations).map(k -> annotatedPrecursor.getAnnotationOrElse(k, "").toString()).collect(Collectors.joining("\t")));
          writer.newLine();
          writer.flush();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return precursors;
  }

  public String[] getAnnotations() {
    return annotations;
  }
}
