package fr.profi.mzknife;

import fr.profi.mzdb.io.writer.mgf.MgfField;
import fr.profi.mzdb.io.writer.mgf.MgfWriter;
import fr.profi.mzknife.recalibration.MGFRecalibrator;
import fr.profi.mzknife.recalibration.RecalibrateUtil;
import fr.profi.mzscope.InvalidMGFFormatException;
import fr.profi.mzscope.MGFConstants;
import fr.profi.mzscope.MGFReader;
import fr.profi.mzscope.MSMSSpectrum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class MGFRewriter {

  private static final DecimalFormat DEC4 = new DecimalFormat("#.####", new DecimalFormatSymbols(Locale.US));
  private static final DecimalFormat DEC5 = new DecimalFormat("#.#####", new DecimalFormatSymbols(Locale.US));

  protected final List<MSMSSpectrum> m_msmsSpectra;
  protected final File m_dstFile;

  private final static Logger LOG = LoggerFactory.getLogger(MGFRewriter.class);

  public MGFRewriter(File srcFile, File m_dstFile) throws InvalidMGFFormatException {
    MGFReader reader = new MGFReader();
    m_msmsSpectra = reader.read(srcFile);
    this.m_dstFile = m_dstFile;
  }

  public void rewriteMGF() throws IOException {

    PrintWriter mgfWriter = new PrintWriter(new BufferedWriter(new FileWriter(m_dstFile)));
    for (MSMSSpectrum spectrum : m_msmsSpectra) {
      MSMSSpectrum outSpectrum =  getSpectrum2Export(spectrum);
      if(outSpectrum != null) {
        String spectrumAsStr = stringifySpectrum(spectrum);
        mgfWriter.print(spectrumAsStr);
        mgfWriter.println();
      }
    }

    mgfWriter.flush();
    mgfWriter.close();
  }


  public String stringifySpectrum(MSMSSpectrum spectrum){
    StringBuilder stb = new StringBuilder();
    stb.append(MgfField.BEGIN_IONS).append(MgfWriter.LINE_SPERATOR);
    stb.append(MgfField.TITLE).append("=").append(spectrum.getAnnotation(MGFConstants.TITLE)).append(MgfWriter.LINE_SPERATOR);

    double precMz = getPrecursorMz(spectrum);

    stb.append(MgfField.PEPMASS).append("=").append(DEC4.format(precMz)).append(MgfWriter.LINE_SPERATOR);
    stb.append(MgfField.CHARGE).append("=").append(spectrum.getPrecursorCharge()).append("+").append(MgfWriter.LINE_SPERATOR);

    Iterator<String> annotations = spectrum.getAnnotations();
    while (annotations.hasNext()) {
      String key = annotations.next();
      if (!key.equals(MGFConstants.TITLE) && !key.equals(MGFConstants.ANNOTATION_CHARGE_STATES))
        stb.append(key).append("=").append(spectrum.getAnnotation(key)).append(MgfWriter.LINE_SPERATOR);
    }

    stb.append(MgfField.RTINSECONDS).append("=").append(spectrum.getRetentionTime()).append(MgfWriter.LINE_SPERATOR);

    double[] masses = getMasses(spectrum) ;
    double[] intensities = spectrum.getIntensityValues();

    for (int k = 0; k < masses.length; k++) {
      stb.append(DEC5.format(masses[k])).append('\t').append(intensities[k]).append(MgfWriter.LINE_SPERATOR);
    }

    stb.append(MgfField.END_IONS).append(MgfWriter.LINE_SPERATOR);


    return stb.toString();
  }


  // Method that may be redefined in subclasses
  // default behaviour no filtering or value processing
  protected MSMSSpectrum getSpectrum2Export(MSMSSpectrum inSpectrum){
    return inSpectrum;
  }

  protected  double getPrecursorMz(MSMSSpectrum spectrum){
    return  spectrum.getPrecursorMz();
  }

  protected  double[] getMasses(MSMSSpectrum spectrum){
    return spectrum.getMassValues();
  }



}
