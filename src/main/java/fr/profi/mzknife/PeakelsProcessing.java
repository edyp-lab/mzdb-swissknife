package fr.profi.mzknife;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.PutativeFeature;
import fr.profi.mzknife.peakeldb.PeakelsDbFinder;
import fr.profi.mzknife.util.AbstractProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class PeakelsProcessing extends AbstractProcessing {

  private final static Logger LOG = LoggerFactory.getLogger(PeakelsProcessing.class);

  /**
   *
   * Search for putative ions in a peakeldb file.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    CommandArguments.IonsMatchingCommand ionsMatchingCommand = new CommandArguments.IonsMatchingCommand();
    CommandArguments.PsmsMatchingCommand psmsMatchingCommand = new CommandArguments.PsmsMatchingCommand();

    addCommand(ionsMatchingCommand);
    addCommand(psmsMatchingCommand);

    try{

      String parsedCommand = parseCommand(args);
      switch (parsedCommand) {
        case CommandArguments.MATCH_IONS_COMMAND_NAME:
          if (ionsMatchingCommand.help)
            usage();

          matchIons(ionsMatchingCommand);
          break;

        case CommandArguments.MATCH_PSMS_COMMAND_NAME:
          if (psmsMatchingCommand.help)
            usage();

          matchPsms(psmsMatchingCommand);
          break;
        default:
          LOG.warn("Invalid command specified ");
          usage();
      }

    } catch (FileNotFoundException fnfe) {
      LOG.error("File not found", fnfe);
    }  catch (Exception e) {
      LOG.error("ERROR ", e);
    }
  }

  private static void matchPsms(CommandArguments.PsmsMatchingCommand psmsMatchingCommand) throws Exception {
    File peakeldb = new File(psmsMatchingCommand.peakelDbFile);
    File putativeIons = new File(psmsMatchingCommand.psmsFile);
    File mzdbFile = new File(psmsMatchingCommand.mzDbFile);
    MzDbReader mzDbReader = new MzDbReader(mzdbFile, true);

    final List<PutativeFeature> putativeFeatures = readPutativeFeatures(new File(psmsMatchingCommand.psmsFile));

    File outputFile = getDestFile(psmsMatchingCommand.outputFile, ".tsv", peakeldb);
    PeakelsDbFinder finder = new PeakelsDbFinder(peakeldb, outputFile);

    finder.matchIdentifiedPsms(putativeFeatures, mzDbReader, psmsMatchingCommand.mzTolPPM);

  }

  public static void matchIons(CommandArguments.IonsMatchingCommand ionsMatchingCommand) throws IOException {

    File peakeldb = new File(ionsMatchingCommand.peakelDbFile);
    File featuredb = null;

    if (ionsMatchingCommand.featureDbFile != null && !ionsMatchingCommand.featureDbFile.isEmpty()) {
      featuredb = new File(ionsMatchingCommand.featureDbFile);
    }

    final List<PutativeFeature> putativeFeatures = readPutativeFeatures(new File(ionsMatchingCommand.putativeIonsFile));

    File outputFile = getDestFile(ionsMatchingCommand.outputFile, ".tsv", peakeldb);
    PeakelsDbFinder finder = new PeakelsDbFinder(peakeldb, outputFile);
    finder.matchPutativeFeatures(putativeFeatures, featuredb, ionsMatchingCommand.mzTolPPM);

  }

  private static List<PutativeFeature> readPutativeFeatures(File file) throws IOException {
    List<PutativeFeature> putativeFeatures = new ArrayList<>();
    final BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
    //
    String line = bufferedReader.readLine();
    line = bufferedReader.readLine();
    while (line != null) {
      if (!line.startsWith("#")) {
        final String[] split = line.split(";");
        final PutativeFeature putativeFeature = new PutativeFeature(Integer.valueOf(split[0]), Double.valueOf(split[1]), Integer.valueOf(split[2]));
        putativeFeature.setElutionTime(Float.valueOf(split[3])*60.0f);
        putativeFeature.setElutionTimeTolerance(Float.valueOf(split[4]));
        putativeFeatures.add(putativeFeature);
      }
      line = bufferedReader.readLine();
    }
    return putativeFeatures;
  }

}
