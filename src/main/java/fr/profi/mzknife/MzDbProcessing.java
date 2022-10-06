package fr.profi.mzknife;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.writer.mgf.*;
import fr.profi.mzknife.mgf.PCleanProcessor;
import fr.profi.mzknife.mzdb.MzDBRecalibrator;
import fr.profi.mzknife.mzdb.MzDBSplitter;
import fr.profi.mzknife.util.AbstractProcessing;
import org.apache.commons.lang3.builder.StandardToStringStyle;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MzDbProcessing extends AbstractProcessing {
  private final static Logger LOG = LoggerFactory.getLogger(MzDbProcessing.class);

  private final static StandardToStringStyle style = new StandardToStringStyle();

  static {
    style.setContentStart("");
    style.setContentEnd("");
    style.setFieldSeparator("\n");
    style.setUseClassName(false);
    style.setUseIdentityHashCode(false);
  }

  public static void main(String[] args) {

    CommandArguments.MzDBRecalibrateCommand mzDBRecalibrateCommand = new CommandArguments.MzDBRecalibrateCommand();
    CommandArguments.MzDBSplitterCommand mzDBSplitterCommand = new CommandArguments.MzDBSplitterCommand();
    CommandArguments.MzDBCreateMgfCommand mzDBCreateMgfCommand  = new CommandArguments.MzDBCreateMgfCommand();

    addCommand(mzDBRecalibrateCommand);
    addCommand(mzDBSplitterCommand);
    addCommand(mzDBCreateMgfCommand);

    try {

          String parsedCmd = parseCommand(args);

          switch (parsedCmd) {
            case CommandArguments.RECALIBRATE_COMMAND_NAME:
              if (mzDBRecalibrateCommand.help)
                usage();

              recalibrateMzdb(mzDBRecalibrateCommand);
              break;

            case CommandArguments.CREATE_MGF_COMMAND_NAME:
              if (mzDBCreateMgfCommand.help)
                usage();

              mzdbcreateMgf(mzDBCreateMgfCommand);
              break;

            case CommandArguments.SPLIT_COMMAND_NAME:
              if (mzDBSplitterCommand.help)
                usage();

              splitMzdb(mzDBSplitterCommand);
              break;

            default:
              LOG.warn("Invalid command specified ");
              usage();
          }
      } catch(FileNotFoundException fnfe){
        LOG.error("File not found", fnfe);
      } catch(SQLiteException sqle){
        LOG.error("mzDB SQLite exception", sqle);
      } catch(Exception e){
        LOG.error("ERROR ", e);
      }
  }

  public static void recalibrateMzdb(CommandArguments.MzDBRecalibrateCommand mzDBRecalibrateCommand) throws ClassNotFoundException, FileNotFoundException, SQLiteException {
    File mzdbSrcFile = new File(mzDBRecalibrateCommand.inputFileName);
    File mzdbDstFile = getDestFile(mzDBRecalibrateCommand.outputFileName, ".recal.mzdb", mzdbSrcFile);
    LOG.info(" MzDbProcessing " + mzdbSrcFile.getName() + " to " + mzdbDstFile.getAbsolutePath());
    MzDbReader srcReader = new MzDbReader(mzdbSrcFile, true);
    MzDBRecalibrator mzdbRecalibrator = new MzDBRecalibrator(srcReader, mzdbDstFile);
    mzdbRecalibrator.recalibrate(mzDBRecalibrateCommand.firstScan, mzDBRecalibrateCommand.lastScan, mzDBRecalibrateCommand.deltaMass);
  }

  public static void splitMzdb(CommandArguments.MzDBSplitterCommand mzDBSplitterCommand) {
    String mzdbInputFileName = mzDBSplitterCommand.inputFileName;
    File mzdbInputFile = new File(mzdbInputFileName);
    MzDBSplitter splitter = new MzDBSplitter(mzdbInputFile);
    boolean splitSuccess = splitter.splitMzDbFile();
    if (!splitSuccess) {
      LOG.warn(" An error occurred during Split. Output files may be corrupted !! ");
    } else {
      List<File> outFiles = splitter.getOutputMzdbFiles();
      LOG.info(" END Splitting {} into {} files : ", mzdbInputFileName, outFiles.size());
      for (File f : outFiles) {
        LOG.info(" Output File: {}", f.getName());
      }
    }
  }

  public static void mzdbcreateMgf(CommandArguments.MzDBCreateMgfCommand mzDBCreateMgfCommand) throws SQLiteException, ClassNotFoundException, IOException {

    LOG.info("Creating MGF File for mzDB file " + mzDBCreateMgfCommand.mzdbFile);
    LOG.info("Precursor m/z values will be defined using the method: " + mzDBCreateMgfCommand.precMzComputation);


    MgfWriter writer = new MgfWriter(mzDBCreateMgfCommand.mzdbFile, mzDBCreateMgfCommand.msLevel);
    Optional<PrecursorMzComputationEnum> precCompEnum = Arrays.stream(PrecursorMzComputationEnum.values()).filter(v -> v.name().equalsIgnoreCase(mzDBCreateMgfCommand.precMzComputation.trim())).findFirst();
    ISpectrumProcessor specProcessor = (mzDBCreateMgfCommand.pClean) ? new PCleanProcessor(mzDBCreateMgfCommand.pCleanLabelMethodName) : new DefaultSpectrumProcessor();

    String s = ToStringBuilder.reflectionToString(mzDBCreateMgfCommand, style);
    List<String> comments = Arrays.stream(s.split(("\n"))).collect(Collectors.toList());
    comments.add("generated on "+ DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").format(LocalDateTime.now())+" by "+System.getProperty("user.name"));

    writer.setHeaderComments(comments);

    if (precCompEnum.isPresent()) {
      writer.write(mzDBCreateMgfCommand.outputFile, new DefaultPrecursorComputer(precCompEnum.get(), mzDBCreateMgfCommand.mzTolPPM), specProcessor, mzDBCreateMgfCommand.intensityCutoff, mzDBCreateMgfCommand.exportProlineTitle);
    } else if (mzDBCreateMgfCommand.precMzComputation.equals("isolation_window_extracted")) {
      IPrecursorComputation precComputer = new IsolationWindowPrecursorExtractor(mzDBCreateMgfCommand.mzTolPPM);
      writer.write(mzDBCreateMgfCommand.outputFile, precComputer, specProcessor, mzDBCreateMgfCommand.intensityCutoff, mzDBCreateMgfCommand.exportProlineTitle);
    } else if (mzDBCreateMgfCommand.precMzComputation.equals("mgf_boost_v3.6")) {
      IPrecursorComputation precComputer = new IsolationWindowPrecursorExtractor_v3_6(mzDBCreateMgfCommand.mzTolPPM);
      writer.write(mzDBCreateMgfCommand.outputFile, precComputer, specProcessor, mzDBCreateMgfCommand.intensityCutoff, mzDBCreateMgfCommand.exportProlineTitle);
//    } else if (mzDBCreateMgfCommand.precMzComputation.equals("isolation_window_extracted_v3.7")) {
//      IPrecursorComputation precComputer = new IsolationWindowPrecursorExtractor_v3_7(mzDBCreateMgfCommand.mzTolPPM);
//      writer.write(mzDBCreateMgfCommand.outputFile, precComputer, specProcessor, mzDBCreateMgfCommand.intensityCutoff, mzDBCreateMgfCommand.exportProlineTitle);
    } else {
      throw new IllegalArgumentException("Can't create the MGF file, invalid precursor m/z computation method");
    }

  }

}
