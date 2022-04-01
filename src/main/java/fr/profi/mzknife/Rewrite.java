package fr.profi.mzknife;

import com.almworks.sqlite4java.SQLiteException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzknife.filter.MGFFilter;
import fr.profi.mzknife.recalibration.MGFRecalibrator;
import fr.profi.mzknife.recalibration.MzdbRecalibrator;
//import fr.profi.mzknife.recalibration.MzdbRecalibratorTLS;
import fr.profi.mzscope.InvalidMGFFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

public class Rewrite {
  private final static Logger LOG = LoggerFactory.getLogger(Rewrite.class);

  public static void main(String[] args) {

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
      if(parsedCmd == null){
        jCmd.usage();
        System.exit(1);
      }
      LOG.info("Running "+parsedCmd+" command ...");

      switch (parsedCmd){
        case RewriterArguments.MZDB_RECAL_COMMAND_NAME:
          if(mzdbRecal.help) {
            jCmd.usage();
          }
          String srcFilePath = mzdbRecal.inputFileName;
          File srcFile = new File(srcFilePath);

          File dstFile = getDestFile(mzdbRecal.outputFileName, ".recal.mzdb", srcFile);
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
          File mgfRecalDstFile = getDestFile(mgfRecal.outputFileName,".recal.mgf",  mgfRecalSrcFile);
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
          File mgfFilterDstFile = getDestFile(mgfFilter.outputFileName, ".filter.mgf", mgfFilterSrcFile);

          List<Integer> charges2Ignore = mgfFilter.charges2Ignore;
          List<Integer> charges2Keep = mgfFilter.charges2Keep;
          // If both or none of parameters charges2Ignore and charges2Keep are set : print error and exit
          if( (charges2Ignore != null  && !charges2Ignore.isEmpty() && charges2Keep != null && !charges2Keep.isEmpty())
                  || ( (charges2Ignore == null || charges2Ignore.isEmpty())  && (charges2Keep == null && charges2Keep.isEmpty())) ){
            LOG.info("One, and only one, of the 2 parameters, --charges and --exclude-charges, should be specified!! ");
            System.exit(1);
          }

          MGFFilter filter = new MGFFilter(mgfFilterSrcFile, mgfFilterDstFile);
          if(charges2Ignore != null) {
            filter.setExcludeCharges(charges2Ignore);
          }else{
            filter.setCharges(charges2Keep);
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

  private static File getDestFile(String outputFile, String defaultExtension, File inputFile){
    String inputFileName = inputFile.getName();
    String dstFilePath = (outputFile != null) ? outputFile : inputFile.getAbsolutePath().substring(0,inputFileName.lastIndexOf('.')) + defaultExtension;
    File dstFile = new File(dstFilePath);
    if (dstFile.exists()) {
      LOG.error("Destination file {} already exists, remove it before running rewrite command", dstFile.getAbsolutePath());
      System.exit(1);
    }
    return  dstFile;
  }
}
