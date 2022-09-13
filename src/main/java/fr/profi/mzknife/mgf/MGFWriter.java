package fr.profi.mzknife.mgf;

import fr.profi.mzdb.io.writer.mgf.MgfField;
import fr.profi.mzscope.MGFConstants;
import fr.profi.mzscope.MSMSSpectrum;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Iterator;
import java.util.Locale;

public class MGFWriter {

  private static final DecimalFormat DEC4 = new DecimalFormat("#.####", new DecimalFormatSymbols(Locale.US));
  private static final DecimalFormat DEC5 = new DecimalFormat("#.#####", new DecimalFormatSymbols(Locale.US));

  public static String LINE_SPERATOR = System.getProperty("line.separator");


  public static String stringifySpectrum(MSMSSpectrum spectrum){
    StringBuilder stb = new StringBuilder();
    stb.append(MgfField.BEGIN_IONS).append(LINE_SPERATOR);
    stb.append(MgfField.TITLE).append("=").append(spectrum.getAnnotation(MGFConstants.TITLE)).append(LINE_SPERATOR);

    double precMz = spectrum.getPrecursorMz();

    stb.append(MgfField.PEPMASS).append("=").append(DEC4.format(precMz)).append(LINE_SPERATOR);
    stb.append(MgfField.CHARGE).append("=").append(spectrum.getPrecursorCharge()).append("+").append(LINE_SPERATOR);

    Iterator<String> annotations = spectrum.getAnnotations();
    while (annotations.hasNext()) {
      String key = annotations.next();
      if (!key.equals(MGFConstants.TITLE) && !key.equals(MGFConstants.ANNOTATION_CHARGE_STATES))
        stb.append(key).append("=").append(spectrum.getAnnotation(key)).append(LINE_SPERATOR);
    }

    stb.append(MgfField.RTINSECONDS).append("=").append(spectrum.getRetentionTime()).append(LINE_SPERATOR);

    double[] masses = spectrum.getMassValues();
    double[] intensities = spectrum.getIntensityValues();

    for (int k = 0; k < masses.length; k++) {
      stb.append(DEC5.format(masses[k])).append('\t').append(intensities[k]).append(LINE_SPERATOR);
    }

    stb.append(MgfField.END_IONS).append(LINE_SPERATOR);


    return stb.toString();
  }

}
