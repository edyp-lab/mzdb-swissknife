package fr.profi.mzknife.mgf;

import fr.profi.mgf.MGFConstants;
import fr.profi.ms.model.MSMSSpectrum;
import fr.profi.mzknife.util.RecalibrateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class MGFRecalibrator extends MGFRewriter {

  private final static Logger LOG = LoggerFactory.getLogger(MGFRecalibrator.class);
  private Double recalLastTime;
  private Double recalFirstTime;
  private Double recalDeltaMass;


  public MGFRecalibrator(File srcFile, File dstFile, Double firstTime, Double lastTime, Double deltaMass) throws IOException {
    super(srcFile,dstFile);

    this.recalDeltaMass =deltaMass;
    this.recalFirstTime = firstTime;
    this.recalLastTime = lastTime;
  }

  protected boolean doRecalibration(MSMSSpectrum spectrum){
    return  (spectrum.getRetentionTime() >= recalFirstTime) && (spectrum.getRetentionTime() <= recalLastTime);
  }

  private double getRecalibratedPrecursorMz(MSMSSpectrum spectrum){
    double newPrecMz = -1;
    newPrecMz = spectrum.getPrecursorMz() + recalDeltaMass * spectrum.getPrecursorMz() / 1000000;
    return  newPrecMz;
  }

  private double[] getRecalibratedMasses(MSMSSpectrum spectrum){
    double[] masses = RecalibrateUtil.recalibrateMasses(spectrum.getMassValues(), recalDeltaMass);
    return masses;
  }


  protected MSMSSpectrum getSpectrum2Export(MSMSSpectrum inSpectrum){
    if (doRecalibration(inSpectrum)) {
      LOG.info("Spectrum {} at RT {} will be recalibrated", inSpectrum.getAnnotation(MGFConstants.TITLE), inSpectrum.getRetentionTime());

      double[] masses = getRecalibratedMasses(inSpectrum);
      double[] intensities = inSpectrum.getIntensityValues();
      MSMSSpectrum outSpectrum = new MSMSSpectrum(
              getRecalibratedPrecursorMz(inSpectrum),
              inSpectrum.getPrecursorIntensity(),
              inSpectrum.getPrecursorCharge(),
              inSpectrum.getRetentionTime());

      inSpectrum.getAnnotations().forEachRemaining(a -> outSpectrum.setAnnotation(a, inSpectrum.getAnnotation(a)));

      for(int k = 0; k < masses.length; k++) {
        outSpectrum.addPeak(masses[k], intensities[k]);
      }

      return outSpectrum;
    }
    return inSpectrum;
  }


}
