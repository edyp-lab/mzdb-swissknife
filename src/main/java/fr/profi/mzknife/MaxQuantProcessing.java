package fr.profi.mzknife;

import fr.profi.mgf.InvalidMGFFormatException;
import fr.profi.ms.model.MSMSSpectrum;
import fr.profi.mzknife.mgf.MGFWriter;
import fr.profi.mzknife.mgf.maxquant.MaxQuantMSMSReader;
import fr.profi.mzknife.util.AbstractProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

public class MaxQuantProcessing extends AbstractProcessing {

  private final static Logger LOG = LoggerFactory.getLogger(MaxQuantProcessing.class);

  /**
   *
   * Use the two files generated by MaxQuant (.sil0.apl and .peak.apl) to create a peaklist file formatted as an MGF file.
   * Usage example :
   *  create_mgf -i1 "C:/Local/bruley/Data/TMT/Lucid/MQ_TMT_8-1_Reprocess20210928/HF1_012692/p0/HF1_012692.HCD.FTMS.sil0.apl" -i2
   *       "C:/Local/bruley/Data/TMT/Lucid/MQ_TMT_8-1_Reprocess20210928/HF1_012692/p0/HF1_012692.HCD.FTMS.peak.apl" -o HF1_012692_MQ.mgf
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    CommandArguments.MaxQuantMGFCommand createMgfCommand = new CommandArguments.MaxQuantMGFCommand();

    addCommand(createMgfCommand);

    try{

      String parsedCommand = parseCommand(args);

      if (parsedCommand.equals(CommandArguments.CREATE_MGF_COMMAND_NAME)) {
        if (createMgfCommand.help)
          usage();

        createMgf(createMgfCommand);
      } else {
        LOG.warn("Invalid command specified ");
        usage();
      }
    } catch (FileNotFoundException fnfe) {
      LOG.error("File not found", fnfe);
    } catch (InvalidMGFFormatException imfe) {
      LOG.error("Invalid MGF file format", imfe);
    } catch (Exception e) {
      LOG.error("ERROR ", e);
    }
  }

  public static void createMgf(CommandArguments.MaxQuantMGFCommand createMgfCommand) throws IOException, InvalidMGFFormatException {

    File in = new File(createMgfCommand.inputFileName1);
    File in2 = new File(createMgfCommand.inputFileName2);
    File mgfDstFile = getDestFile(createMgfCommand.outputFileName, ".mgf", in);

    MaxQuantMSMSReader reader = new MaxQuantMSMSReader();
    PrintWriter mgfWriter = new PrintWriter(new BufferedWriter(new FileWriter(mgfDstFile)));

    List<MSMSSpectrum> msmsSpectra = reader.read(in);
    for (MSMSSpectrum spectrum : msmsSpectra) {
      String spectrumAsStr = MGFWriter.stringifySpectrum(spectrum);
      mgfWriter.print(spectrumAsStr);
      mgfWriter.println();
    }

    List<MSMSSpectrum> msmsSpectra2 = reader.read(in2);
    for (MSMSSpectrum spectrum : msmsSpectra2) {
      String spectrumAsStr = MGFWriter.stringifySpectrum(spectrum);
      mgfWriter.print(spectrumAsStr);
      mgfWriter.println();
    }

    mgfWriter.flush();
    mgfWriter.close();

  }

}
