package fr.profi.mzknife.recalibration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recalibrate a mzdb file
 */
public class RecalibrateUtil {

  private final static Logger LOG = LoggerFactory.getLogger(RecalibrateUtil.class);

  public static double[] recalibrateMasses(double[] masses, double deltaMass) {
    double[] recalibratedMz = new double[masses.length];
    for (int i = 0; i < masses.length; i++) {
      recalibratedMz[i] = masses[i] + deltaMass * masses[i] / 1000000;
    }
    return recalibratedMz;
  }

}
