package fr.profi.mzknife.recalibration;

import fr.profi.mzdb.io.writer.mgf.MgfField;
import fr.profi.mzdb.io.writer.mgf.MgfWriter;
import fr.profi.mzscope.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class MGFRecalibrator {


  private final List<MSMSSpectrum> m_msmsSpectra;
  private final File m_dstFile;
  private static final DecimalFormat DEC4 = new DecimalFormat("#.####", new DecimalFormatSymbols(Locale.US));
  private static final DecimalFormat DEC5 = new DecimalFormat("#.#####", new DecimalFormatSymbols(Locale.US));
  private final static Logger LOG = LoggerFactory.getLogger(MGFRecalibrator.class);


  public MGFRecalibrator(File srcFile, File dstFile) throws InvalidMGFFormatException {
    MGFReader reader = new MGFReader();
    m_msmsSpectra = reader.read(srcFile);
    m_dstFile = dstFile;

  }


  public void recalibrate(Double firstTime, Double lastTime, Double deltaMass) throws IOException {

    PrintWriter mgfWriter = new PrintWriter(new BufferedWriter(new FileWriter(m_dstFile)));
    for (MSMSSpectrum spectrum : m_msmsSpectra) {

      String spectrumAsStr = stringifySpectrum(spectrum, firstTime, lastTime, deltaMass);
      mgfWriter.print(spectrumAsStr);
      mgfWriter.println();

    }

    mgfWriter.flush();
    mgfWriter.close();

  }

  public String stringifySpectrum(MSMSSpectrum spectrum, Double firstTime, Double lastTime, Double deltaMass) {

    StringBuilder stb = new StringBuilder();
    stb.append(MgfField.BEGIN_IONS).append(MgfWriter.LINE_SPERATOR);
    stb.append(MgfField.TITLE).append("=").append(spectrum.getAnnotation(MGFConstants.TITLE)).append(MgfWriter.LINE_SPERATOR);

    double precMz = -1.0;
    boolean recalibrate = (spectrum.getRetentionTime() >= firstTime) && (spectrum.getRetentionTime() <= lastTime);
    if (recalibrate) {
      precMz = spectrum.getPrecursorMz() + deltaMass * spectrum.getPrecursorMz() / 1000000;
    } else {
      precMz = spectrum.getPrecursorMz();
    }

    stb.append(MgfField.PEPMASS).append("=").append(DEC4.format(precMz)).append(MgfWriter.LINE_SPERATOR);
    stb.append(MgfField.CHARGE).append("=").append(spectrum.getPrecursorCharge()).append("+").append(MgfWriter.LINE_SPERATOR);

    Iterator<String> annotations = spectrum.getAnnotations();
    while (annotations.hasNext()) {
      String key = annotations.next();
      if (!key.equals(MGFConstants.TITLE) && !key.equals(MGFConstants.ANNOTATION_CHARGE_STATES))
        stb.append(key).append("=").append(spectrum.getAnnotation(key)).append(MgfWriter.LINE_SPERATOR);
    }

    stb.append(MgfField.RTINSECONDS).append("=").append(spectrum.getRetentionTime()).append(MgfWriter.LINE_SPERATOR);

    double[] masses = (recalibrate) ? Recalibrate.recalibrate(spectrum.getMassValues(), deltaMass): spectrum.getMassValues();
    double[] intensities = spectrum.getIntensityValues();

    for (int k = 0; k < masses.length; k++) {
      stb.append(DEC5.format(masses[k])).append('\t').append(intensities[k]).append(MgfWriter.LINE_SPERATOR);
    }

    stb.append(MgfField.END_IONS).append(MgfWriter.LINE_SPERATOR);

    if (recalibrate) {
      LOG.info("Spectrum {} at RT {} has been recalibrated", spectrum.getAnnotation(MGFConstants.TITLE), spectrum.getRetentionTime());
    }

    return stb.toString();
  }
}
