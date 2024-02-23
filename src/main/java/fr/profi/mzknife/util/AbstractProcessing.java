package fr.profi.mzknife.util;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class AbstractProcessing {

  private final static Logger LOG = LoggerFactory.getLogger(AbstractProcessing.class);

  static final JCommander jCmd = new JCommander();


  public static String parseCommand(String[] args) {

    try {
      jCmd.parse(args);
      String parsedCmd = jCmd.getParsedCommand();
      if (parsedCmd == null) {
        jCmd.usage();
        System.exit(1);
      }
      LOG.info("Running " + parsedCmd + " command ...");
      return parsedCmd;
    } catch (MissingCommandException mce) {
      LOG.warn("Invalid command specified ");
      usage();
    }
    return "";
  }

  public static void addCommand(Object command) {
    jCmd.addCommand(command);
  }

  protected static File getDestFile(String outputFile, String defaultExtension, File inputFile){
    String inputFileName = inputFile.getAbsolutePath();
    String dstFilePath = (outputFile != null) ? outputFile : inputFileName.substring(0,inputFileName.lastIndexOf('.')) + defaultExtension;
    File dstFile = new File(dstFilePath);
    if (dstFile.exists()) {
      LOG.error("Destination file {} already exists, remove it before running rewrite command", dstFile.getAbsolutePath());
      System.exit(1);
    }
    return  dstFile;
  }

  protected static void usage() {
    jCmd.usage();
    System.exit(0);
  }
}
