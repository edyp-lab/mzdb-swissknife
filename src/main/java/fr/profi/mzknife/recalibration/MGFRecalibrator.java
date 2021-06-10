package fr.profi.mzknife.recalibration;

import fr.profi.mzknife.MGFRewriter;
import fr.profi.mzscope.InvalidMGFFormatException;
import fr.profi.mzscope.MGFConstants;
import fr.profi.mzscope.MSMSSpectrum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class MGFRecalibrator extends MGFRewriter {

  private final static Logger LOG = LoggerFactory.getLogger(MGFRecalibrator.class);
  private Double recalLastTime;
  private Double recalFirstTime;
  private Double recalDeltaMass;


  public MGFRecalibrator(File srcFile, File dstFile, Double firstTime, Double lastTime, Double deltaMass) throws InvalidMGFFormatException {
    super(srcFile,dstFile);

    this.recalDeltaMass =deltaMass;
    this.recalFirstTime = firstTime;
    this.recalLastTime = lastTime;
  }

  protected boolean doRecalibration(MSMSSpectrum spectrum){
    return  (spectrum.getRetentionTime() >= recalFirstTime) && (spectrum.getRetentionTime() <= recalLastTime);
  }

  protected  double getPrecursorMz(MSMSSpectrum spectrum){
    double newPrecMz = -1;
    boolean recalibrate = doRecalibration(spectrum);
    if (recalibrate) {
      newPrecMz = spectrum.getPrecursorMz() + recalDeltaMass * spectrum.getPrecursorMz() / 1000000;
    } else {
      newPrecMz = spectrum.getPrecursorMz();
    }
    return  newPrecMz;
  }

  protected MSMSSpectrum getSpectrum2Export(MSMSSpectrum inSpectrum){
    if (doRecalibration(inSpectrum)) {
      LOG.info("Spectrum {} at RT {} will be recalibrated", inSpectrum.getAnnotation(MGFConstants.TITLE), inSpectrum.getRetentionTime());
    }
    return inSpectrum;
  }

  protected  double[] getMasses(MSMSSpectrum spectrum){
    boolean recalibrate = doRecalibration(spectrum);

    double[] masses = (recalibrate) ? RecalibrateUtil.recalibrateMasses(spectrum.getMassValues(), recalDeltaMass): spectrum.getMassValues();

    return masses;
  }


}
