package fr.profi.mzknife;

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

    CommandArguments.PeakelsFinderCommand peakelFinderCommand = new CommandArguments.PeakelsFinderCommand();

    addCommand(peakelFinderCommand);

    try{

      String parsedCommand = parseCommand(args);

      if (parsedCommand.equals(CommandArguments.PEAKELS_COMMAND_NAME)) {
        if (peakelFinderCommand.help)
          usage();

        findPeakels(peakelFinderCommand);
      } else {
        LOG.warn("Invalid command specified ");
        usage();
      }
    } catch (FileNotFoundException fnfe) {
      LOG.error("File not found", fnfe);
    }  catch (Exception e) {
      LOG.error("ERROR ", e);
    }
  }

  public static void findPeakels(CommandArguments.PeakelsFinderCommand peakelsFinderCommand) throws IOException {

    File peakeldb = new File(peakelsFinderCommand.peakeldbFile);
    File putativeIons = new File(peakelsFinderCommand.putativeIonsFile);
    File outputFile = getDestFile(peakelsFinderCommand.outputFile, ".tsv", peakeldb);

    List<PutativeFeature> putativeFeatures = new ArrayList<>();
    final BufferedReader bufferedReader = new BufferedReader(new FileReader(putativeIons));
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

    PeakelsDbFinder finder = new PeakelsDbFinder(peakeldb, outputFile);
    finder.findPeakels(putativeFeatures, peakelsFinderCommand.mzTolPPM);


  }

}
