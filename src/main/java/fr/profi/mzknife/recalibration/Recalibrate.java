package fr.profi.mzknife.recalibration;

import com.almworks.sqlite4java.SQLiteException;
import com.beust.jcommander.JCommander;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzscope.InvalidMGFFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Recalibrate a mzdb file
 */
public class Recalibrate {

  private final static Logger LOG = LoggerFactory.getLogger(Recalibrate.class);

  public static double[] recalibrate(double[] masses, double deltaMass) {
    double[] recalibratedMz = new double[masses.length];
    for (int i = 0; i < masses.length; i++) {
      recalibratedMz[i] = masses[i] + deltaMass * masses[i] / 1000000;
    }
    return recalibratedMz;
  }

  public static void main(String[] args) {

    RecalibrateArguments recalArgs = new RecalibrateArguments();

    JCommander jc = JCommander.newBuilder()
            .addObject(recalArgs)
            .build();

    try {
      jc.parse(args);
      if (recalArgs.help || (recalArgs.inputFileName == null)) {
        jc.usage();
        System.exit(1);
      }
    } catch (RuntimeException re) {
      jc.usage();
      System.exit(1);
    }

    try {
      String srcFilePath = recalArgs.inputFileName;
      File srcFile = new File(srcFilePath);
      Optional<String> extension = getExtensionByStringHandling(srcFilePath);
      if (!extension.isPresent()) {
        LOG.error("Input file type cannot be inferred from the filename, please use .mzdb or .mgf extensions");
        System.exit(1);
      } else if (extension.get().equalsIgnoreCase("mzdb")) {
        String dstFilePath = (recalArgs.outputFileName != null) ? recalArgs.outputFileName : srcFilePath.substring(0, srcFilePath.lastIndexOf('.')) + ".recal.mzdb";
        File dstFile = new File(dstFilePath);

        if (dstFile.exists()) {
          LOG.error("Destination file {} already exists, remove it before running recalibration", dstFile.getAbsolutePath());
          System.exit(1);
        }

        //TODO move to arguments validation
        if (recalArgs.firstTime != Double.MIN_VALUE && recalArgs.lastTime != Double.MAX_VALUE) {
          LOG.error("Only first_scan argument is supported for mzdb recalibration, please specify the first scan from which the recalibration must be applied and/or remove first_time argument");
          System.exit(1);
        }

        MzDbReader srcReader = new MzDbReader(srcFile, true);
        MzdbRecalibrator instance = new MzdbRecalibrator(srcReader, dstFile);
        instance.recalibrate(recalArgs.firstScan, recalArgs.lastScan, recalArgs.deltaMass);

      } else if (extension.get().equalsIgnoreCase("mgf")) {

        String dstFilePath = (recalArgs.outputFileName != null) ? recalArgs.outputFileName : srcFilePath.substring(0, srcFilePath.lastIndexOf('.')) + ".recal.mgf";
        File dstFile = new File(dstFilePath);

        if (dstFile.exists()) {
          LOG.error("Destination file {} already exists, remove it before running recalibration", dstFile.getAbsolutePath());
          System.exit(1);
        }

        //TODO move to arguments validation
        if (recalArgs.firstScan != Long.MIN_VALUE && recalArgs.lastScan != Long.MAX_VALUE) {
          LOG.error("Only first_time argument is supported for mgf recalibration, please specify the first time from which the recalibration must be applied and/or remove first_scan argument");
          System.exit(1);
        }

        MGFRecalibrator instance = new MGFRecalibrator(srcFile, dstFile);
        instance.recalibrate(recalArgs.firstTime, recalArgs.lastTime, recalArgs.deltaMass);

      } else {
        LOG.error("Only .mzdb or .mgf input files are supported");
        System.exit(1);
      }

    } catch (FileNotFoundException fnfe) {
      LOG.error("File not found", fnfe);
    } catch (ClassNotFoundException cnfe) {
      LOG.error("sqlite classes not found", cnfe);
    } catch (SQLiteException sqle) {
      LOG.error("SQLite exception", sqle);
    } catch (InvalidMGFFormatException imfe) {
      LOG.error("Invalid MGF file format", imfe);
    } catch (IOException ioe) {
      LOG.error("IO Exception occurred", ioe);
    }
  }

  private static Optional<String> getExtensionByStringHandling(String filename) {
    return Optional.ofNullable(filename)
            .filter(f -> f.contains("."))
            .map(f -> f.substring(filename.lastIndexOf(".") + 1));
  }
}
