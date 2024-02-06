package fr.profi.mzknife;

import com.almworks.sqlite4java.SQLiteException;
import com.beust.jcommander.ParameterException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.Software;
import fr.profi.mzdb.io.writer.mgf.*;
import fr.profi.mzknife.mgf.PCleanProcessor;
import fr.profi.mzknife.mzdb.MzDBRecalibrator;
import fr.profi.mzknife.mzdb.MzDBSplitter;
import fr.profi.mzknife.util.AbstractProcessing;
import fr.profi.mzknife.util.PrecursorComputerWrapper;
import org.apache.commons.lang3.builder.StandardToStringStyle;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Enumeration;

import java.io.*;
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

  public static void recalibrateMzdb(CommandArguments.MzDBRecalibrateCommand mzDBRecalibrateCommand) throws FileNotFoundException, SQLiteException {
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

  public static void mzdbcreateMgf(CommandArguments.MzDBCreateMgfCommand mzDBCreateMgfCommand) throws SQLiteException, IOException {

    final Config config = ConfigFactory.load();

    LOG.info("Creating MGF File for mzDB file " + mzDBCreateMgfCommand.mzdbFile);
    LOG.info("Precursor m/z values will be defined using the method: " + mzDBCreateMgfCommand.precMzComputation);

    MgfWriter writer = new MgfWriter(mzDBCreateMgfCommand.mzdbFile, mzDBCreateMgfCommand.msLevel);

    MzDbReader mzDbReader = writer.getMzDbReader();

    // --- Get and verify parameters for pClean (they should be consistent).
    // --- Define SpectrumProcessor to use and configure it
    ISpectrumProcessor specProcessor = createSpectrumProcessor(mzDBCreateMgfCommand);

    // --- Define which PrecursorComputation method to use.
    IPrecursorComputation precursorComputation = createPrecursorComputation(mzDBCreateMgfCommand);

    //Call writer to create mgf
    String s = ToStringBuilder.reflectionToString(mzDBCreateMgfCommand, style);
    List<String> comments = Arrays.stream(s.split(("\n"))).collect(Collectors.toList());
    CommandArguments.PCleanConfig pCleanConfig = mzDBCreateMgfCommand.pCleanConfig;
    if(pCleanConfig != null){
      comments.addAll(pCleanConfig.getPCleanConfigTemplate().stringifyParametersList());
    }
    comments.add("precMzComputation.description="+precursorComputation.getMethodName()+" - version "+precursorComputation.getMethodVersion());
    comments.add("input.mzdb.format.version="+mzDbReader.getModelVersion());
    Software soft = mzDbReader.getMzdbConverter();
    if(soft!=null) {
      comments.add("input.mzdb.converter.software=" + soft.getName());
      comments.add("input.mzdb.converter.version=" + soft.getVersion());
    }

    comments.add("generated on "+ DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").format(LocalDateTime.now())+" by "+System.getProperty("user.name"));

    writer.setHeaderComments(comments);
    writer.write(mzDBCreateMgfCommand.outputFile, precursorComputation, specProcessor, mzDBCreateMgfCommand.intensityCutoff, mzDBCreateMgfCommand.exportProlineTitle);
  }

  private static IPrecursorComputation createPrecursorComputation(CommandArguments.MzDBCreateMgfCommand mzDBCreateMgfCommand) {
    IPrecursorComputation precursorComputation;
    Optional<PrecursorMzComputationEnum> precCompEnum = Arrays.stream(PrecursorMzComputationEnum.values()).filter(v -> v.name().equalsIgnoreCase(mzDBCreateMgfCommand.precMzComputation.trim())).findFirst();
    if (precCompEnum.isPresent()) {
      // if precCompEnum define, use mzdb-access mgf generator.
      precursorComputation = new DefaultPrecursorComputer(precCompEnum.get(), mzDBCreateMgfCommand.mzTolPPM);
    } else if (mzDBCreateMgfCommand.precMzComputation.equals("isolation_window_extracted")) {
      // specif precursor method (isolation_window_extracted)
      precursorComputation = new IsolationWindowPrecursorExtractor(mzDBCreateMgfCommand.mzTolPPM);
    } else if (mzDBCreateMgfCommand.precMzComputation.equals("mgf_boost")) {
      // specif precursor method (mgfBoost)
      final Enumeration.Value scanSelector = ScanSelectorModes.withName(mzDBCreateMgfCommand.scanSelectorMode.toString());
      precursorComputation =  new MgfBoostPrecursorExtractor(mzDBCreateMgfCommand.mzTolPPM,
                                                             mzDBCreateMgfCommand.useHeader,
                                                             mzDBCreateMgfCommand.useSelectionWindow,
                                                             mzDBCreateMgfCommand.swMaxPrecursorsCount,
                                                             mzDBCreateMgfCommand.swIntensityThreshold,
                                                             scanSelector,
                                                             mzDBCreateMgfCommand.pifThreshold,
                                                             mzDBCreateMgfCommand.rankThreshold);
    } else {
      throw new IllegalArgumentException("Can't create the MGF file, invalid precursor m/z computation method");
    }

    if(mzDBCreateMgfCommand.dAnnot) {
      try {

        final BufferedWriter annotationWriter = new BufferedWriter(new FileWriter(mzDBCreateMgfCommand.outputFile+".annot"));
        PrecursorComputerWrapper wrapper = new PrecursorComputerWrapper(precursorComputation, annotationWriter);
        annotationWriter.write(Arrays.stream(wrapper.getAnnotations()).collect(Collectors.joining(wrapper.getDelimiter())));
        annotationWriter.newLine();
        precursorComputation = wrapper;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }


    return precursorComputation;
  }

  private static ISpectrumProcessor createSpectrumProcessor(CommandArguments.MzDBCreateMgfCommand createMgfCommand){

    CommandArguments.PCleanConfig pCleanConfig = createMgfCommand.pCleanConfig;
    boolean usePClean = createMgfCommand.pClean;
    String pCleanMethod = createMgfCommand.pCleanLabelMethodName;

    ISpectrumProcessor specProcessor = null;
    if(usePClean){
      if(pCleanConfig == null) {
        LOG.error("if pClean usage is specified, the -pConfig parameter should be specified ! ");
        usage();
      } else {
        switch (pCleanConfig){
          case XLINK:
          case LABEL_FREE: {
            if(pCleanMethod != null && !pCleanMethod.isEmpty()) {
              LOG.warn("Specified pClean method " + pCleanMethod + " will be ignored. It isn't consistent with " + pCleanConfig.getConfigCommandValue());
              pCleanMethod = "";
            }
            break;
          }
          case TMT_LABELED:
            if(pCleanMethod == null || pCleanMethod.isEmpty()) {
              throw new ParameterException("When using "+pCleanConfig.getConfigCommandValue()+" configuration, you must specify pClean label method using -pLabelMethod");
            }
        } //end switch
        specProcessor = new PCleanProcessor(pCleanMethod);
        ((PCleanProcessor)specProcessor).setPCleanParameters(pCleanConfig.getPCleanConfigTemplate());
      }

    } else {
      specProcessor = new DefaultSpectrumProcessor();
    }
    return specProcessor;
  }

}
