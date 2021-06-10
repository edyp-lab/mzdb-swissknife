package fr.profi.mzknife;

import com.almworks.sqlite4java.SQLiteException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzknife.filter.MGFFilter;
import fr.profi.mzknife.recalibration.MGFRecalibrator;
import fr.profi.mzknife.recalibration.MzdbRecalibrator;
import fr.profi.mzscope.InvalidMGFFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;

public class Rewrite {
  private final static Logger LOG = LoggerFactory.getLogger(Rewrite.class);

  public static void main(String[] args) {

    RewriterArguments recalArgs = new RewriterArguments();
    RewriterArguments.MgfRecalibrateCommand mgfRecal = new RewriterArguments.MgfRecalibrateCommand();
    RewriterArguments.MzDBRecalibrateCommand mzdbRecal = new RewriterArguments.MzDBRecalibrateCommand();
    RewriterArguments.MgfFilterCommand mgfFilter = new RewriterArguments.MgfFilterCommand();
    JCommander jCmd = new JCommander();
    jCmd.addCommand(mgfRecal);
    jCmd.addCommand(mzdbRecal);
    jCmd.addCommand(mgfFilter);
    try {
      jCmd.parse(args);
      String parsedCmd = jCmd.getParsedCommand();
      LOG.info("Running "+parsedCmd+" command ...");

      switch (parsedCmd){
        case RewriterArguments.MZDB_RECAL_COMMAND_NAME:
          if(mzdbRecal.help) {
            jCmd.usage();
          }
          String srcFilePath = mzdbRecal.inputFileName;
          File srcFile = new File(srcFilePath);

          File dstFile = getDestFile(mzdbRecal.outputFileName, srcFile);
          LOG.info(" Rewrite "+srcFilePath+" to "+dstFile.getAbsolutePath());
          MzDbReader srcReader = new MzDbReader(srcFile, true);
          MzdbRecalibrator mzdbRecalibrator = new MzdbRecalibrator(srcReader, dstFile);
          mzdbRecalibrator.recalibrate(mzdbRecal.firstScan, mzdbRecal.lastScan, mzdbRecal.deltaMass);

          break;

        case RewriterArguments.MGF_RECAL_COMMAND_NAME:
          if(mgfRecal.help)
            jCmd.usage();

          String mgfRecalSrcFilePath = mgfRecal.inputFileName;
          File mgfRecalSrcFile = new File(mgfRecalSrcFilePath);
          File mgfRecalDstFile = getDestFile(mgfRecal.outputFileName, mgfRecalSrcFile);
          MGFRecalibrator mgfRecalibrator = new MGFRecalibrator(mgfRecalSrcFile, mgfRecalDstFile,mgfRecal.firstTime, mgfRecal.lastTime, mgfRecal.deltaMass);
          mgfRecalibrator.rewriteMGF();

          break;

        case  RewriterArguments.MGF_FILTER_COMMAND_NAME:
          if(mgfFilter.help) {
            jCmd.usage();
            break;
          }
          String mgfFilterSrcFilePath = mgfFilter.inputFileName;
          File mgfFilterSrcFile = new File(mgfFilterSrcFilePath);
          File mgfFilterDstFile = getDestFile(mgfFilter.outputFileName, mgfFilterSrcFile);

          Integer ignoreCharge = mgfFilter.ignoreCharge;
          Integer keepCharge = mgfFilter.keptCharge;
          if((ignoreCharge != null && keepCharge != null) ||(ignoreCharge == null && keepCharge == null)){
            LOG.info("One, and only one, of the 2 parameters, --charge and --exclude-charge, should be specified!! ");
            System.exit(1);
          }
          MGFFilter filter = new MGFFilter(mgfFilterSrcFile, mgfFilterDstFile);
          if(ignoreCharge != null) {
            filter.setExcludeCharge(ignoreCharge);
          }else{
            filter.setCharge(keepCharge);
          }
          filter.rewriteMGF();
          break;

        default:
          LOG.warn("Invalid commande specified ");
          jCmd.usage();
      }

    } catch (MissingCommandException mce) {
      LOG.warn("Invalid command specified ");
      jCmd.usage();
    } catch (FileNotFoundException fnfe) {
      LOG.error("File not found", fnfe);
    } catch (SQLiteException sqle) {
      LOG.error("mzDB SQLite exception", sqle);
    } catch (InvalidMGFFormatException imfe) {
      LOG.error("Invalid MGF file format", imfe);
    } catch (Exception e) {
      LOG.error("ERROR ", e);
    }

  }

  private static File getDestFile(String outputFile, File inputFile){
    String inputFileName = inputFile.getName();
    String dstFilePath = (outputFile != null) ? outputFile : inputFile.getAbsolutePath().substring(0,inputFileName.lastIndexOf('.')) + ".recal.mzdb";
    File dstFile = new File(dstFilePath);
    if (dstFile.exists()) {
      LOG.error("Destination file {} already exists, remove it before running rewrite command", dstFile.getAbsolutePath());
      System.exit(1);
    }
    return  dstFile;
  }
}
