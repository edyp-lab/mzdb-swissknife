package fr.profi.mzknife;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzknife.peakeldb.PeakelsDbFinder;
import fr.profi.mzknife.peakeldb.PutativeFeatureWrapper;
import fr.profi.mzknife.util.AbstractProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class PeakelsProcessing extends AbstractProcessing {

  private final static Logger LOG = LoggerFactory.getLogger(PeakelsProcessing.class);

  private enum Column{ ID, MOZ, CHARGE, RT, SEQ, PTMS, TTOL }

  public static class ColumnT {
    private Column column;
    private int index;

    public ColumnT(Column c) {
      column = c;
      index = c.ordinal();
    }

    public boolean isPresent() {
      return this.index >= 0;
    }

  }

  public static class InputSource {
    private List<PutativeFeatureWrapper> putativeFeatures;
    private String originalHeader;
    private Map<Integer, String> originalLines;

    public InputSource(List<PutativeFeatureWrapper> putativeFeatures, String originalHeader, Map<Integer, String> originalLines) {
      this.putativeFeatures = putativeFeatures;
      this.originalHeader = originalHeader;
      this.originalLines = originalLines;
    }

    public List<PutativeFeatureWrapper> getPutativeFeatures() {
      return putativeFeatures;
    }

    public String getOriginalHeader() {
      return originalHeader;
    }

    public Map<Integer, String> getOriginalLines() {
      return originalLines;
    }
  }
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

  private static void matchPsms(CommandArguments.PsmsMatchingCommand command) throws Exception {

    Map<Column, ColumnT> columns = new HashMap<>() {{
      put(Column.ID, new ColumnT(Column.ID));
      put(Column.MOZ, new ColumnT(Column.MOZ));
      put(Column.CHARGE, new ColumnT(Column.CHARGE));
      put(Column.RT, new ColumnT(Column.RT));
      put(Column.SEQ, new ColumnT(Column.SEQ));
      put(Column.PTMS, new ColumnT(Column.PTMS));
    }};

    if ((command.columnsConfig != null) && (!command.columnsConfig.isEmpty())) {
        initializeColumns(command.columnsConfig, columns);
    }

    File peakeldb = new File(command.peakelDbFile);
    File mzdbFile = new File(command.mzDbFile);
    MzDbReader mzDbReader = new MzDbReader(mzdbFile, true);

    final InputSource source = readPutativeFeatures(new File(command.psmsFile), columns);

    if (source.getPutativeFeatures().size() != source.getOriginalLines().size()) {
      LOG.error("The id is not unique ({} ids vs {} lines)", source.getOriginalLines().size(), source.getPutativeFeatures().size());
      return;
    }

    File outputFile = getDestFile(command.outputFile, ".tsv", peakeldb);
    PeakelsDbFinder finder = new PeakelsDbFinder(peakeldb, outputFile);

    if (command.groupIons && !(columns.get(Column.SEQ).isPresent() && columns.get(Column.PTMS).isPresent())) {
      LOG.error("To group PSMs the sequence and modifications columns must be provided");
      return;
    }

    finder.matchIdentifiedPsms(source, mzDbReader, command.mzTolPPM, command.groupIons);

  }

  public static void matchIons(CommandArguments.IonsMatchingCommand command) throws Exception {

    Map<Column, ColumnT> columns = new HashMap<>() {{
      put(Column.ID, new ColumnT(Column.ID));
      put(Column.MOZ, new ColumnT(Column.MOZ));
      put(Column.CHARGE, new ColumnT(Column.CHARGE));
      put(Column.RT, new ColumnT(Column.RT));
      put(Column.SEQ, new ColumnT(Column.SEQ));
      put(Column.PTMS, new ColumnT(Column.PTMS));
      put(Column.TTOL, new ColumnT(Column.TTOL));
    }};

    if ((command.columnsConfig != null) && (!command.columnsConfig.isEmpty())) {
      initializeColumns(command.columnsConfig, columns);
    }

    File peakeldb = new File(command.peakelDbFile);
    File featuredb = null;

    if (command.featureDbFile != null && !command.featureDbFile.isEmpty()) {
      featuredb = new File(command.featureDbFile);
    }

    final InputSource source = readPutativeFeatures(new File(command.putativeIonsFile), columns);

    File outputFile = getDestFile(command.outputFile, ".tsv", peakeldb);
    PeakelsDbFinder finder = new PeakelsDbFinder(peakeldb, outputFile);
    finder.matchPutativeFeatures(source, featuredb, command.mzTolPPM);

  }


  private static InputSource readPutativeFeatures(File file, Map<Column, ColumnT> columns) throws IOException, CsvValidationException {
    List<PutativeFeatureWrapper> putativeFeatures = new ArrayList<>();
    Map<Integer, String> initialInput = new HashMap<>();

    final CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
    CSVReader reader = new CSVReaderBuilder(new FileReader(file)).withCSVParser(parser).build();
    int lineCount = 0;
    String header = String.join(";",reader.readNext());
    String[] split;
    while ((split = reader.readNext()) != null) {

        final Integer id = columns.get(Column.ID).isPresent() ? Integer.valueOf(split[columns.get(Column.ID).index]) : lineCount;

        final double moz = Double.valueOf(split[columns.get(Column.MOZ).index]);
        final int charge = Integer.valueOf(split[columns.get(Column.CHARGE).index]);
        final PutativeFeatureWrapper putativeFeature = new PutativeFeatureWrapper(id, moz, charge);
        final float elutionTime = Float.valueOf(split[columns.get(Column.RT).index]);
        putativeFeature.setElutionTime(elutionTime*60.0f);

        if (columns.get(Column.SEQ).isPresent() && columns.get(Column.PTMS).isPresent()) {
          String sequence = split[columns.get(Column.SEQ).index];
          String modifications = split[columns.get(Column.PTMS).index];
          putativeFeature.setSequenceModifications(sequence, modifications);
        }

        if (columns.containsKey(Column.TTOL) && columns.get(Column.TTOL).isPresent())
          putativeFeature.setElutionTimeTolerance(Float.valueOf(split[columns.get(Column.TTOL).index]));

        putativeFeatures.add(putativeFeature);
        StringBuilder strBuilder = new StringBuilder();
        Arrays.stream(split).forEach(s -> {
          if (s.contains(";")) {
            strBuilder.append('"').append(s).append('"').append(";");
          } else {
            strBuilder.append(s).append(";");
          }
        });
        strBuilder.setLength(Math.max(strBuilder.length()-1, 0));
        initialInput.put(id, strBuilder.toString());
        lineCount++;
      }
    return new InputSource(putativeFeatures, header, initialInput);
  }

  private static void initializeColumns(String filePath,  Map<Column, ColumnT> columns) {
    try {
      FileInputStream fis = new FileInputStream(filePath);
      Properties properties = new Properties();
      properties.load(fis);
      for( Column c : columns.keySet()) {
        if (properties.containsKey(c.name())) {
          String value = properties.getProperty(c.name());
          if ((value != null) && !value.isEmpty()) {
            columns.get(c).index = Integer.valueOf(value);
          } else {
            columns.get(c).index = -1;
          }
        }
      }
    } catch (IOException ioe) {
      LOG.error("Column properties cannot be read from {}",filePath);
    }
  }

}
