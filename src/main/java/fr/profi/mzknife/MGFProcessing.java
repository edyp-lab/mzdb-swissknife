package fr.profi.mzknife;

import Preprocessing.*;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import fr.profi.mzknife.mgf.*;
import fr.profi.mzknife.util.AbstractProcessing;
import fr.profi.mzscope.InvalidMGFFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class MGFProcessing extends AbstractProcessing {
  private final static Logger LOG = LoggerFactory.getLogger(MGFProcessing.class);

  public static void main(String[] args) {

    CommandArguments.MgfRecalibrateCommand mgfRecalibrateCommand = new CommandArguments.MgfRecalibrateCommand();
    CommandArguments.MgfFilterCommand mgfFilterCommand = new CommandArguments.MgfFilterCommand();
    CommandArguments.MgfCleanerCommand mgfCleanerCommand  = new CommandArguments.MgfCleanerCommand();
    CommandArguments.MgfMergerCommand mgfMergerCommand  = new CommandArguments.MgfMergerCommand();
    CommandArguments.PCleanCommand pCleanCommand = new CommandArguments.PCleanCommand();

    addCommand(mgfRecalibrateCommand);
    addCommand(mgfFilterCommand);
    addCommand(mgfCleanerCommand);
    addCommand(mgfMergerCommand);
    addCommand(pCleanCommand);

    try {

      String parsedCmd = parseCommand(args);

      switch (parsedCmd) {
        case CommandArguments.RECALIBRATE_COMMAND_NAME:
          if (mgfRecalibrateCommand.help)
            usage();

          recalibrateMgf(mgfRecalibrateCommand);
          break;

        case CommandArguments.FILTER_COMMAND_NAME:
          if (mgfFilterCommand.help)
            usage();

          filterMgf(mgfFilterCommand);
          break;

        case CommandArguments.CLEAN_COMMAND_NAME:
          if (mgfCleanerCommand.help)
            usage();

          cleanMgf(mgfCleanerCommand);
          break;

        case CommandArguments.MERGE_COMMAND_NAME:
          if (mgfMergerCommand.help)
            usage();

          mergeMgf(mgfMergerCommand);
          break;

        case CommandArguments.PCLEAN_COMMAND_NAME:
          if (pCleanCommand.help)
            usage();

          pCleanMgf(pCleanCommand);
          break;

        default:
          LOG.warn("Invalid command specified ");
          usage();
      }
    } catch(FileNotFoundException fnfe){
      LOG.error("File not found", fnfe);
    } catch(InvalidMGFFormatException imfe){
      LOG.error("Invalid MGF file format", imfe);
    } catch(Exception e){
      LOG.error("ERROR ", e);
    }
  }

  public static void pCleanMgf(CommandArguments.PCleanCommand pCleanCommand) throws InvalidMGFFormatException, IOException {
    File mgfPCleanSrcFile = new File(pCleanCommand.mgf);
    File mgfPCleanDstFile = getDestFile(pCleanCommand.outputFileName, ".pclean.mgf", mgfPCleanSrcFile);
    PCleanProcessor pCleanProcessor = new PCleanProcessor(mgfPCleanSrcFile, mgfPCleanDstFile, pCleanCommand.labelMethod);
    pCleanProcessor.setPCleanParameters(pCleanCommand.ionFilter, pCleanCommand.repFilter, pCleanCommand.labelFilter, pCleanCommand.low, pCleanCommand.high, pCleanCommand.isoReduction, pCleanCommand.chargeDeconv, pCleanCommand.ionsMerge, pCleanCommand.largerThanPrecursor);
    DeltaMassDB.consider2aa = pCleanCommand.aa2;
    Config.ms2tol = pCleanCommand.itol;
    pCleanProcessor.rewriteMGF();
  }

  public static void mergeMgf(CommandArguments.MgfMergerCommand mgfMergerCommand) throws InvalidMGFFormatException, IOException {
    File mgfMergerSrcFile = new File(mgfMergerCommand.inputFileName1);
    File mgfMergerFragmentsFile = new File(mgfMergerCommand.inputFileName2);
    File mgfMergerDstFile = getDestFile(mgfMergerCommand.outputFileName, ".merge.mgf", mgfMergerSrcFile);
    MGFMerger mgfMerger = new MGFMerger(mgfMergerSrcFile, mgfMergerFragmentsFile, mgfMergerDstFile);
    mgfMerger.setFilterSpectrum(mgfMergerCommand.filter);
    mgfMerger.setReplaceFragments(mgfMergerCommand.replace);
    mgfMerger.rewriteMGF();
    mgfMerger.dumpMetric();
  }

  public static void cleanMgf(CommandArguments.MgfCleanerCommand mgfCleanerCommand) throws InvalidMGFFormatException, IOException {
    File mgfCleanerSrcFile = new File(mgfCleanerCommand.inputFileName);
    File mgfCleanerDstFile = getDestFile(mgfCleanerCommand.outputFileName, ".clean.mgf", mgfCleanerSrcFile);
    MGFCleaner mgfCleaner = new MGFCleaner(mgfCleanerSrcFile, mgfCleanerDstFile);
    mgfCleaner.rewriteMGF();
  }

  public static void recalibrateMgf(CommandArguments.MgfRecalibrateCommand mgfRecalibrateCommand) throws InvalidMGFFormatException, IOException {
    File mgfRecalSrcFile = new File(mgfRecalibrateCommand.inputFileName);
    File mgfRecalDstFile = getDestFile(mgfRecalibrateCommand.outputFileName, ".recal.mgf", mgfRecalSrcFile);
    MGFRecalibrator mgfRecalibrator = new MGFRecalibrator(mgfRecalSrcFile, mgfRecalDstFile, mgfRecalibrateCommand.firstTime, mgfRecalibrateCommand.lastTime, mgfRecalibrateCommand.deltaMass);
    mgfRecalibrator.rewriteMGF();
  }

  public static void filterMgf(CommandArguments.MgfFilterCommand mgfFilterCommand) throws InvalidMGFFormatException, IOException {
    File mgfFilterSrcFile = new File(mgfFilterCommand.inputFileName);
    File mgfFilterDstFile = getDestFile(mgfFilterCommand.outputFileName, ".filter.mgf", mgfFilterSrcFile);

    List<Integer> charges2Ignore = mgfFilterCommand.charges2Ignore;
    List<Integer> charges2Keep = mgfFilterCommand.charges2Keep;
    // If both or none of parameters charges2Ignore and charges2Keep are set : print error and exit
    if ((charges2Ignore != null && !charges2Ignore.isEmpty() && charges2Keep != null && !charges2Keep.isEmpty())
            || ((charges2Ignore == null || charges2Ignore.isEmpty()) && (charges2Keep == null && charges2Keep.isEmpty()))) {
      LOG.info("One, and only one, of the 2 parameters, --charges and --exclude-charges, should be specified!! ");
      System.exit(1);
    }

    MGFFilter filter = new MGFFilter(mgfFilterSrcFile, mgfFilterDstFile);
    if (charges2Ignore != null) {
      filter.setExcludeCharges(charges2Ignore);
    } else {
      filter.setCharges(charges2Keep);
    }
    filter.rewriteMGF();
  }

}
