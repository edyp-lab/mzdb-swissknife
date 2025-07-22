package fr.profi.mzknife;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import fr.profi.mzdb.FeatureDetectorConfig;
import fr.profi.mzdb.MzDbFeatureDetector;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.SmartPeakelFinderConfig;
import fr.profi.mzdb.model.*;
import fr.profi.mzdb.peakeldb.io.PeakelDbWriter;
import fr.profi.mzknife.peakeldb.FeatureWriter;
import fr.profi.mzknife.peakeldb.ConsensusIon;
import fr.profi.mzknife.peakeldb.PeakelsDbFinder;
import fr.profi.mzknife.peakeldb.PutativeFeatureWrapper;
import fr.profi.mzknife.util.AbstractProcessing;
import fr.profi.mzknife.util.LcMsRunSliceIteratorFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class PeakelsProcessing extends AbstractProcessing {

  private final static Logger LOG = LoggerFactory.getLogger(PeakelsProcessing.class);

  private enum Column {ID, MOZ, CHARGE, RT, SEQ, PTMS, SCAN_NUMBER, TTOL, CV, RAWFILE}

  public static class ColumnT {
    private Column column;
    private int index;
    private List<Integer> indexes = null;

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
   * Search for putative ions in a peakeldb file.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    CommandArguments.IonsMatchingCommand ionsMatchingCommand = new CommandArguments.IonsMatchingCommand();
    CommandArguments.PsmsMatchingCommand psmsMatchingCommand = new CommandArguments.PsmsMatchingCommand();
    CommandArguments.QuantifyPsmsCommand quantifyPsmsCommand = new CommandArguments.QuantifyPsmsCommand();

    addCommand(ionsMatchingCommand);
    addCommand(psmsMatchingCommand);
    addCommand(quantifyPsmsCommand);

    try {

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

        case CommandArguments.QUANTIFY_PSMS_COMMAND_NAME:
          if (quantifyPsmsCommand.help)
            usage();

          quantifyPsms(quantifyPsmsCommand);
          break;

        default:
          LOG.warn("Invalid command specified ");
          usage();
      }

    } catch (FileNotFoundException fnfe) {
      LOG.error("File not found", fnfe);
    } catch (Exception e) {
      LOG.error("ERROR ", e);
    }
  }

  private static void quantifyPsms(CommandArguments.QuantifyPsmsCommand command) throws Exception {

    Map<Column, ColumnT> columns = initializeColumns(command.columnsConfig);

    if (!columns.get(Column.RAWFILE).isPresent()) {
      LOG.error("To match PSMs to mzdb files the RAW_FILE column must be provided in the configuration file");
      return;
    }

    final File psmsFile = new File(command.psmsFile);
    final File outputPSMsFile = getDestFile(command.outputFile, "_matched_psms.csv", psmsFile);
    final File outputMissingIonsFile = getDestFile(command.outputFile, "_matched_ions.csv", psmsFile);
    final InputSource source = readPutativeFeatures(psmsFile, columns);

    // force ion key update using the CV value if present
    source.getPutativeFeatures().forEach(pf -> pf.updateKeys(true));

    final Map<String, List<PutativeFeatureWrapper>> putativeFtsByRun = source.getPutativeFeatures().stream().collect(Collectors.groupingBy(pf -> pf.getRawSourceFile()));

    Map<String, File> mzdbFilesByRawFileName = new HashMap<>();

    // check that mzdb file can be found in the <directory> folder
    for(String rawFileName : putativeFtsByRun.keySet()) {
      String mzdbFileName = rawFileName.substring(0,rawFileName.lastIndexOf('.')) + ".mzdb";
      File mzdbFile = new File(command.directory, mzdbFileName);
      if (! mzdbFile.exists()) {
        LOG.error(".mzdb file corresponding to {} cannot be found in {} ({})", rawFileName, command.directory, mzdbFile.getAbsolutePath());
        return;
      } else {
        mzdbFilesByRawFileName.put(rawFileName, mzdbFile);
      }
    }

    // Match PSMs by Run
    List<PutativeFeatureWrapper> psms = new ArrayList<>(source.getPutativeFeatures().size());
    for(Map.Entry<String, File> entry : mzdbFilesByRawFileName.entrySet()) {
      CommandArguments.PsmsMatchingCommand psmCommand = new CommandArguments.PsmsMatchingCommand();
      psmCommand.mzDbFile = entry.getValue().getAbsolutePath();
      psmCommand.mzTolPPM = command.mzTolPPM;
      psmCommand.groupPsms = command.groupPsms;
      psmCommand.peakelDbFile = null;

      final List<PutativeFeatureWrapper> matchedPsms = _matchPsms(psmCommand, new InputSource(putativeFtsByRun.get(entry.getKey()), source.originalHeader, source.originalLines));
      psms.addAll(matchedPsms);
    }

    if (command.writeMatchedPsms || (!command.crossAssign && !command.groupPsms)) {
      FeatureWriter.writeFeatures(outputPSMsFile, psms, source.getOriginalLines(), source.getOriginalHeader(), true);
    }

    if (command.groupPsms || command.crossAssign) {

      // Group ions across runs into consensus ions.
      // if command.crossAssign = true then detect missing values and creates putative features for them
      final List<String> allRuns = putativeFtsByRun.keySet().stream().sorted().toList();
      long startGrouping = System.currentTimeMillis();
      int missingCount = 0;
      final Map<String, List<PutativeFeatureWrapper>> psmsByIonKey = psms.stream().collect(Collectors.groupingBy(PutativeFeatureWrapper::getIonKey));
      final Map<String, List<PutativeFeatureWrapper>> missingFeaturesByRun = new HashMap<>();
      final List<ConsensusIon> consensusIons = new ArrayList<>(psmsByIonKey.size());

      for (Map.Entry<String, List<PutativeFeatureWrapper>> e : psmsByIonKey.entrySet()) {

        final ConsensusIon consensusIon = new ConsensusIon(e.getValue());
        consensusIons.add(consensusIon);

        if (command.crossAssign) {
          final List<String> identifiedRuns = consensusIon.getMatchedRuns();

          if (identifiedRuns.size() != allRuns.size()) {
            TreeSet<String> missingRuns = new TreeSet<>(allRuns);
            missingRuns.removeAll(new TreeSet<>(identifiedRuns));

            final List<PutativeFeatureWrapper> matchedIonfeatures = consensusIon.getMatchedFeatures();
            final PutativeFeatureWrapper representativeFeature = consensusIon.getRepresentativeFeature();

            if (!matchedIonfeatures.isEmpty()) {
              for (String run : missingRuns) {
                final List<PutativeFeatureWrapper> missingFeatures = missingFeaturesByRun.getOrDefault(run, new ArrayList<>());
                // Creates a clone missing Feature ion from quantified ones
                PutativeFeatureWrapper missingFeature = new PutativeFeatureWrapper(-(missingCount+1), matchedIonfeatures.stream().mapToDouble(PutativeFeature::mz).average().getAsDouble(), representativeFeature.charge());
                missingFeature.setElutionTime((float) matchedIonfeatures.stream().mapToDouble(pf -> pf.getRepresentativeExperimentalFeature().getElutionTime()).average().getAsDouble());
                missingFeature.setSequenceModifications(representativeFeature.getSequence(), representativeFeature.getModification());
                missingFeature.setCvValue(representativeFeature.getCvValue());
                missingFeature.setRawSourceFile(run);
                missingFeature.setElutionTimeTolerance(command.rtTolerance);

                missingFeatures.add(missingFeature);
                missingFeaturesByRun.put(run, missingFeatures);
                consensusIon.addMissingFeature(run, missingFeature);
                missingCount++;
              }
            } else {
              LOG.warn("No experimental feature matched for {}, cannot create missing feature", consensusIon.getRepresentativeFeature().getIonKey());
            }
          }
        }
      }

      LOG.info("Missing detection duration : {} ms to generate {} ions from {} psms, {} missing detected", (System.currentTimeMillis() - startGrouping), consensusIons.size(), source.getPutativeFeatures().size(), missingCount);

      if (command.crossAssign) {
        // Match missing features
        for (Map.Entry<String, File> mzdbFileMapEntry : mzdbFilesByRawFileName.entrySet()) {
          final List<PutativeFeatureWrapper> missingFeatures = missingFeaturesByRun.get(mzdbFileMapEntry.getKey());

          File mzdbFile = mzdbFileMapEntry.getValue();
          MzDbReader mzDbReader = new MzDbReader(mzdbFile, true);
          mzDbReader.enableParamTreeLoading();
          mzDbReader.enablePrecursorListLoading();
          mzDbReader.enableScanListLoading();

          final IonMobilityMode mobilityMode = mzDbReader.getIonMobilityMode();
          final Map<String, File> peakeldbFilesMap = buildPeakeldbFilesMap(null, command.mzTolPPM, mzDbReader, mzdbFileMapEntry.getValue());

          // iterate over all peakelDbs
          for (Map.Entry<String, File> peakelDbFilesMapEntry : peakeldbFilesMap.entrySet()) {
            List<PutativeFeatureWrapper> missing;
            if (mobilityMode == null) {
              missing = missingFeatures;
            } else {
              missing = missingFeatures.stream().filter(pf -> pf.getCvValue().equals(peakelDbFilesMapEntry.getKey())).collect(Collectors.toList());
            }
            LOG.info("Matching {} ions on peakels from {}", missing.size(), peakelDbFilesMapEntry.getValue().getAbsolutePath());
            PeakelsDbFinder finder = new PeakelsDbFinder(peakelDbFilesMapEntry.getValue());
            final List<PutativeFeatureWrapper> matchedIons = finder.matchPutativeFeatures(missing, null, command.mzTolPPM);
          }
        }
      }
      if (command.groupPsms) {
        LOG.info("Writing {} ions in {}", consensusIons.size(), outputMissingIonsFile.getAbsolutePath());
        FeatureWriter.writeIons(outputMissingIonsFile, consensusIons, source.getOriginalLines(), source.getOriginalHeader(), true, allRuns);
      } else {
        final List<PutativeFeatureWrapper> allPsms = consensusIons.stream().map(ConsensusIon::getAllPutativeFeatures).flatMap(Collection::stream).toList();
        FeatureWriter.writeFeatures(outputMissingIonsFile, allPsms, source.getOriginalLines(), source.getOriginalHeader(), true);
      }
    }
  }

  private static void matchPsms(CommandArguments.PsmsMatchingCommand command) throws Exception {

    Map<Column, ColumnT> columns = initializeColumns(command.columnsConfig);

    // check configuration consistency : if grouping is requested, SEQ and PTMS columns must have been supplied
    if (command.groupPsms && !(columns.get(Column.SEQ).isPresent() && columns.get(Column.PTMS).isPresent())) {
      LOG.error("To group PSMs the sequence and modifications columns must be provided");
      return;
    }

    final File psmsFile = new File(command.psmsFile);
    File outputFile = getDestFile(command.outputFile, "_matched.tsv", psmsFile);
    final InputSource source = readPutativeFeatures(psmsFile, columns);
    final List<PutativeFeatureWrapper> psms = _matchPsms(command, source);
    FeatureWriter.writeFeatures(outputFile, psms, source.getOriginalLines(), source.getOriginalHeader(), command.outputUnassignedPsms);
  }

  private static List<PutativeFeatureWrapper> _matchPsms(CommandArguments.PsmsMatchingCommand command, InputSource source) throws Exception {

    File mzdbFile = new File(command.mzDbFile);
    MzDbReader mzDbReader = new MzDbReader(mzdbFile, true);
    mzDbReader.enableParamTreeLoading();
    mzDbReader.enablePrecursorListLoading();
    mzDbReader.enableScanListLoading();

    final IonMobilityMode mobilityMode = mzDbReader.getIonMobilityMode();
    final Map<String, File> peakeldbFilesMap = buildPeakeldbFilesMap(command.peakelDbFile, command.mzTolPPM, mzDbReader, mzdbFile);

    // iterate over all peakelDbs
    List<PutativeFeatureWrapper> psms = new ArrayList<>(source.getPutativeFeatures().size());

    for (Map.Entry<String, File> entry : peakeldbFilesMap.entrySet()) {
      List<PutativeFeatureWrapper> putativeFeatures;
      if (mobilityMode == null) {
         putativeFeatures = source.getPutativeFeatures();
      } else {
         putativeFeatures = source.getPutativeFeatures().stream().filter(pf -> pf.getCvValue().equals(entry.getKey())).collect(Collectors.toList());
      }
      LOG.info("Matching {} PSMs on peakels from {}", putativeFeatures.size(), entry.getValue().getAbsolutePath());
      PeakelsDbFinder finder = new PeakelsDbFinder(entry.getValue());
      final List<PutativeFeatureWrapper> matchedPsms = finder.matchIdentifiedPsms(putativeFeatures, mzDbReader, command.mzTolPPM, command.groupPsms);
      psms.addAll(matchedPsms);
    }
    return psms;
  }

  private static Map<String, File> buildPeakeldbFilesMap(String peakelDbFile, Float mzTolPPM, MzDbReader mzDbReader, File mzdbFile) throws SQLiteException {
    Map<String, File> peakeldbFilesMap = null;

    // search for or auto-generate peakeldb file if not specified in the command
    if (peakelDbFile == null) {
        LOG.info("PeakeldDb file not supplied, searching for it or generating it");
        peakeldbFilesMap = generatePeakelDb(mzDbReader, mzdbFile, mzTolPPM);
    } else {
      peakeldbFilesMap = new HashMap<>();
      peakeldbFilesMap.put("", new File(peakelDbFile));
    }
    return peakeldbFilesMap;
  }

  public static void matchIons(CommandArguments.IonsMatchingCommand command) throws Exception {

    Map<Column, ColumnT> columns = initializeColumns(command.columnsConfig);

    File featuredb = null;
    if (command.featureDbFile != null && !command.featureDbFile.isEmpty()) {
      featuredb = new File(command.featureDbFile);
    }

    final File ionsFile = new File(command.putativeIonsFile);
    File outputFile = getDestFile(command.outputFile, ".tsv", ionsFile);
    final InputSource source = readPutativeFeatures(ionsFile, columns);
    File peakeldb = new File(command.peakelDbFile);
    PeakelsDbFinder finder = new PeakelsDbFinder(peakeldb);
    final List<PutativeFeatureWrapper> ions = finder.matchPutativeFeatures(source.getPutativeFeatures(), featuredb, command.mzTolPPM);

    FeatureWriter.writeFeatures(outputFile, ions, source.getOriginalLines(), source.getOriginalHeader(), command.outputUnassignedIons);
  }


  private static InputSource readPutativeFeatures(File file, Map<Column, ColumnT> columns) throws IOException, CsvValidationException {
    List<PutativeFeatureWrapper> putativeFeatures = new ArrayList<>();
    Map<Integer, String> initialInput = new HashMap<>();

    final CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
    CSVReader reader = new CSVReaderBuilder(new FileReader(file)).withCSVParser(parser).build();
    int lineCount = 0;
    String header = String.join(";", reader.readNext());
    String[] split;
    while ((split = reader.readNext()) != null) {

      try {
        final Integer id = columns.get(Column.ID).isPresent() ? Integer.valueOf(split[columns.get(Column.ID).index]) : lineCount;

        final double moz = Double.valueOf(split[columns.get(Column.MOZ).index]);
        final int charge = Integer.valueOf(split[columns.get(Column.CHARGE).index]);
        final PutativeFeatureWrapper putativeFeature = new PutativeFeatureWrapper(id, moz, charge);

        if (columns.get(Column.RT).isPresent()) {
          final float elutionTime = Float.valueOf(split[columns.get(Column.RT).index]);
          putativeFeature.setElutionTime(elutionTime * 60.0f);
        }
        if (columns.get(Column.SCAN_NUMBER).isPresent()) {
          final long scanNumber = Long.valueOf(split[columns.get(Column.SCAN_NUMBER).index]);
          putativeFeature.setSpectrumId(scanNumber);
        }

        if (columns.get(Column.SEQ).isPresent() && columns.get(Column.PTMS).isPresent()) {
          String modificationAsStr;
          String sequence = split[columns.get(Column.SEQ).index];
          List<String> modifications = new ArrayList<>();
          if (columns.get(Column.PTMS).indexes != null) {
            for (Integer ptmIndex : columns.get(Column.PTMS).indexes) {
              modifications.add(split[ptmIndex]);
            }
            modificationAsStr = modifications.stream().collect(Collectors.joining("."));
          } else {
            modificationAsStr = split[columns.get(Column.PTMS).index];
          }
          putativeFeature.setSequenceModifications(sequence, modificationAsStr);
        }

        if (columns.containsKey(Column.TTOL) && columns.get(Column.TTOL).isPresent())
          putativeFeature.setElutionTimeTolerance(Float.valueOf(split[columns.get(Column.TTOL).index]));

        if (columns.containsKey(Column.CV) && columns.get(Column.CV).isPresent())
          putativeFeature.setCvValue(split[columns.get(Column.CV).index]);

        if (columns.containsKey(Column.RAWFILE) && columns.get(Column.RAWFILE).isPresent())
          putativeFeature.setRawSourceFile(split[columns.get(Column.RAWFILE).index]);

        putativeFeatures.add(putativeFeature);

        // protect original content with quotes
        StringBuilder strBuilder = new StringBuilder();
        Arrays.stream(split).forEach(s -> {
          if (s.contains(";")) {
            strBuilder.append('"').append(s).append('"').append(";");
          } else {
            strBuilder.append(s).append(";");
          }
        });
        strBuilder.setLength(Math.max(strBuilder.length() - 1, 0));
        initialInput.put(id, strBuilder.toString());
        lineCount++;
      } catch (NumberFormatException nfe) {
        LOG.error("Error line {}: cannot read column number value", lineCount);
        LOG.error("Number format exception", nfe);
      }

    }
    final InputSource source = new InputSource(putativeFeatures, header, initialInput);

    if (source.getPutativeFeatures().size() != source.getOriginalLines().size()) {
      // means that the ID column was specified in the configuration file, but the ID is not unique.
      LOG.error("The id is not unique ({} ids vs {} lines)", source.getOriginalLines().size(), source.getPutativeFeatures().size());
      System.exit(1);
    }

    return source;
  }

  private static Map<Column, ColumnT> initializeColumns(String configurationFilePath) {

    Map<Column, ColumnT> columns = new HashMap<>() {{
      put(Column.ID, new ColumnT(Column.ID));
      put(Column.MOZ, new ColumnT(Column.MOZ));
      put(Column.CHARGE, new ColumnT(Column.CHARGE));
      put(Column.RT, new ColumnT(Column.RT));
      put(Column.SCAN_NUMBER, new ColumnT(Column.SCAN_NUMBER));
      put(Column.SEQ, new ColumnT(Column.SEQ));
      put(Column.PTMS, new ColumnT(Column.PTMS));
      put(Column.CV, new ColumnT(Column.CV));
      put(Column.RAWFILE, new ColumnT(Column.RAWFILE));
      put(Column.TTOL, new ColumnT(Column.TTOL));
    }};

    // initialize columns indexes by reading the configuration file (if supplied)
    if ((configurationFilePath != null) && !configurationFilePath.isEmpty()) {
      try {
        FileInputStream fis = new FileInputStream(configurationFilePath);
        Properties properties = new Properties();
        properties.load(fis);
        for (Column c : columns.keySet()) {
          if (properties.containsKey(c.name())) {
            String value = properties.getProperty(c.name());
            if ((value != null) && !value.isEmpty()) {
                final String[] split = value.split("\\+");
                columns.get(c).index = Integer.valueOf(split[0].trim());
                if (split.length > 1) {
                  final List<Integer> list = List.of(split).stream().map( s -> Integer.valueOf(s.trim())).toList();
                  columns.get(c).indexes = list;
                }
            } else {
              columns.get(c).index = -1;
            }
          } else {
            columns.get(c).index = -1;
          }
        }
      } catch (IOException ioe) {
        LOG.error("Column properties cannot be read from {}", configurationFilePath);
      }
    }
    return columns;
  }

  private static Map<String, File> generatePeakelDb(MzDbReader mzDbReader, File mzdbFile, Float mzTol) throws SQLiteException {

    Map<String, File> peakeldbFilesMap = new HashMap<>();

    final IonMobilityMode mobilityMode = mzDbReader.getIonMobilityMode();
    final List<String> values = mobilityMode == null ? List.of("") : mobilityMode.getSeparationValues();

    for (String value : values) {

      String extPrefix = (mobilityMode == null) ? value : "_" + value;
      File peakelsDbFile = inferFile(null, extPrefix + ".peakelDb", mzdbFile);

      if (peakelsDbFile.exists()) {
        LOG.info("Existing PeakelDb file found : {}", peakelsDbFile.getAbsolutePath());
        peakeldbFilesMap.put(value, peakelsDbFile);
      } else {
        try {
          LOG.info("PeakelDb not found : start generation");
          MzDbFeatureDetector detector = new MzDbFeatureDetector(mzDbReader, new FeatureDetectorConfig(
                  1,
                  mzTol,
                  5,
                  0.9f,
                  3,
                  new SmartPeakelFinderConfig(
                          3,
                          3,
                          0.75f,
                          false,
                          10,
                          false,
                          false,
                          true
                  )
          ));

          // hack : for some stupid reason, CV values are reported as integer in the method but as 3 digits float in Spectra
          String cvValueFilter = value+".000";

          final Iterator<RunSlice> sliceIterator = (mobilityMode == null) ? mzDbReader.getLcMsRunSliceIterator() : new LcMsRunSliceIteratorFilter(mzDbReader.getLcMsRunSliceIterator(), cvValueFilter);
          Peakel[] peakels = detector.detectPeakels(sliceIterator, Option.empty());
          LOG.info("{} peakels detected", peakels.length);
          peakelsDbFile.createNewFile();
          final SQLiteConnection peakelFileConnection = PeakelDbWriter.initPeakelStore(peakelsDbFile);
          PeakelDbWriter.storePeakelsInPeakelDB(peakelFileConnection, peakels, mzDbReader.getMs1SpectrumHeaders());
          peakelFileConnection.dispose();
          LOG.info("PeakelDb file generated : {}", peakelsDbFile.getAbsolutePath());
          peakeldbFilesMap.put(value, peakelsDbFile);

        } catch (Exception e) {
          LOG.error("Error while writing peakels", e);
        }
      }
    }
    return peakeldbFilesMap;
  }

}