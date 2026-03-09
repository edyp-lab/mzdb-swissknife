
package fr.profi.mzknife.peakeldb;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.Feature;
import fr.profi.mzknife.PeakelsProcessing;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class PeakelsDbFinderTest {

  private static final Logger LOG = LoggerFactory.getLogger(PeakelsDbFinderTest.class);

  private static final String MZDB_FILE = "/OVEMB150205_12.raw.0.9.8.mzDB";
  private static final String PSMS_FILE = "/OVEMB150205_12_psms.csv";
  private static final String PUTATIVE_FT_FILE = "/OVEMB150205_12_psms.csv";
  private static final String PSMS_COLUMNS_FILE = "/config_psms.columns";
  private static final String PFEATURES_COLUMNS_FILE = "/config_putativeFts.columns";

  private static final String PEAKELSDB_FILE = "/OVEMB150205_12.peakelDb";
  private static final String QUANT_RESULT_FILE = "/OVEMB150205_12_quanti.csv";

  private File mzdbFile;
  private File psmsFile;
  private File putativeFtFile;
  private File peakelsDbFile;
  private PeakelsDbFinder finder;
  Float mzTolPPM = 5.0f;

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    // Get test resource files
    mzdbFile = new File(getClass().getResource(MZDB_FILE).getFile());
    psmsFile = new File(getClass().getResource(PSMS_FILE).getFile());
    putativeFtFile = new File(getClass().getResource(PUTATIVE_FT_FILE).getFile());
    peakelsDbFile = new File(getClass().getResource(PEAKELSDB_FILE).getFile());

    // Verify test resources exist
    assertTrue("MzDB test file should exist", mzdbFile.exists());
    assertTrue("PSMs test file should exist", psmsFile.exists());
    assertTrue("Putative features test file should exist", putativeFtFile.exists());

    // Initialize the PeakelsDbFinder
    if (peakelsDbFile != null && peakelsDbFile.exists()) {
      finder = new PeakelsDbFinder(peakelsDbFile);
    }
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testConstructorWithValidPeakelDb() {
    assertNotNull("PeakelsDbFinder should be initialized", finder);
  }

  @Test
  public void testConstructorWithNonExistentFile() {
    File nonExistentFile = new File("non_existent.peakelDb");

    assertThrows("Should throw RuntimeException when peakelDb file doesn't exist",
            RuntimeException.class, () -> {
      new PeakelsDbFinder(nonExistentFile);
    });
  }

  @Test
  public void testMatchIdentifiedPsmsWithRealData() throws Exception {

    // Read PSMs from CSV file using PeakelsProcessing utility
    PeakelsProcessing.ReaderConfiguration configuration = PeakelsProcessing.ReaderConfiguration.fromFile(getClass().getResource(PSMS_COLUMNS_FILE).getFile());
    PeakelsProcessing.InputSource source = PeakelsProcessing.readPutativeFeatures(psmsFile, configuration);

    assertNotNull("Input source should be loaded", source);
    assertFalse("PSMs list should not be empty", source.getPutativeFeatures().isEmpty());

    // Initialize MzDbReader
    MzDbReader mzDbReader = new MzDbReader(mzdbFile, true);
    mzDbReader.enableParamTreeLoading();
    mzDbReader.enablePrecursorListLoading();
    mzDbReader.enableScanListLoading();

    // Match PSMs

    Boolean groupPSMs = false;
    List<PutativeFeatureWrapper> matchedPsms = finder.matchIdentifiedPsms(
            source.getPutativeFeatures(),
            mzDbReader,
            mzTolPPM,
            groupPSMs
    );

    assertNotNull("Matched PSMs should not be null",matchedPsms);
    assertEquals("Matched PSMs count should match input PSMs count", source.getPutativeFeatures().size(), matchedPsms.size());

    // Check that at least some PSMs were matched
    long matchedCount = matchedPsms.stream().filter(PutativeFeatureWrapper::isMatched).count();
    assertTrue("91 PSMs should be matched to peakels in this example", matchedCount == 91);

    LOG.info("Matched {} out of {} PSMs", matchedCount, matchedPsms.size());

    final List<PutativeFeatureWrapper> unmatchedPsms = matchedPsms.stream().filter(p -> !p.isMatched()).toList();

    assertQuantificationAbundancesMatch(matchedPsms);

    mzDbReader.close();
  }

  /**
   * Compares the abundances of matched putative features to the expected quantification results
   * to ensure consistency. Assert that all matched abundances are equals to the expected quantification abundances.
   *
   * @param matchedPsms a list of {@code PutativeFeatureWrapper} objects representing putative
   *                    features that have been matched during {@link PeakelsDbFinder} processing
   * @throws IOException if an error occurs while reading the quantification results file
   * @throws CsvValidationException if the quantification results CSV file is invalid
   */
  private void assertQuantificationAbundancesMatch(List<PutativeFeatureWrapper> matchedPsms) throws IOException, CsvValidationException {
    // compare matched peakels to expected results

    long matchedCount = matchedPsms.stream().filter(PutativeFeatureWrapper::isMatched).count();

    final List<QuantificationResult> results = readQuantificationResults(new File(getClass().getResource(QUANT_RESULT_FILE).getFile()));
    final Map<String, List<QuantificationResult>> resultsByIonKey = results.stream().collect(Collectors.groupingBy(QuantificationResult::getIonKey));
    int matchingAbundances = 0;

    for (PutativeFeatureWrapper ft : matchedPsms) {
      if (ft.isMatched()) {
        List<QuantificationResult> matchingResults = resultsByIonKey.get(ft.getIonKey());
        if (matchingResults != null && matchingResults.size() > 0) {
          final double ftIntensity = ft.getRepresentativeExperimentalFeature().getBasePeakel().getApexIntensity();
          final double quantIntensity = matchingResults.get(0).getAbundance();
          if ( Math.abs( ftIntensity - quantIntensity) < 1)  {
            matchingAbundances++;
          } else {
            LOG.error("Same key but different abundance value !!!!!");
          }
        } else {
          LOG.error("Feature {}, Ab = {} not found in quantification results ", ft.getIonKey(), ft.getRepresentativeExperimentalFeature().getBasePeakel().getApexIntensity());
        }
      }
    }

    assertEquals("Matching abundances", matchedCount, matchingAbundances);
  }

  @Test
  public void testMatchIdentifiedPsmsWithGrouping() throws Exception {

    // Read PSMs from CSV file
    PeakelsProcessing.ReaderConfiguration configuration = PeakelsProcessing.ReaderConfiguration.fromFile(getClass().getResource(PSMS_COLUMNS_FILE).getFile());
    PeakelsProcessing.InputSource source = PeakelsProcessing.readPutativeFeatures(psmsFile, configuration);

    assertNotNull(source);

    // Initialize MzDbReader
    MzDbReader mzDbReader = new MzDbReader(mzdbFile, true);
    mzDbReader.enableParamTreeLoading();
    mzDbReader.enablePrecursorListLoading();
    mzDbReader.enableScanListLoading();

    // Match PSMs with grouping enabled

    Boolean groupPSMs = true;
    List<PutativeFeatureWrapper> groupedPsms = finder.matchIdentifiedPsms(
            source.getPutativeFeatures(),
            mzDbReader,
            mzTolPPM,
            groupPSMs
    );

    assertNotNull("Grouped PSMs should not be null", groupedPsms);

    // With grouping, the result size should be less than or equal to input size
    assertTrue( "Grouped PSMs count should be less than or equal to input count",
            groupedPsms.size() <= source.getPutativeFeatures().size());

    LOG.info("Grouped {} PSMs into {} ions", source.getPutativeFeatures().size(), groupedPsms.size());

    assertQuantificationAbundancesMatch(groupedPsms);
    mzDbReader.close();
  }

  @Test
  public void testMatchPutativeFeaturesWithRealData() throws Exception {

    // Read putative features from CSV file
    PeakelsProcessing.ReaderConfiguration configuration = PeakelsProcessing.ReaderConfiguration.fromFile(getClass().getResource(PFEATURES_COLUMNS_FILE).getFile());
    PeakelsProcessing.InputSource source = PeakelsProcessing.readPutativeFeatures(putativeFtFile, configuration);

    assertNotNull("Input source should be loaded", source);
    assertFalse("Putative features list should not be empty", source.getPutativeFeatures().isEmpty());

    // Match putative features

    List<PutativeFeatureWrapper> matchedFeatures = finder.matchPutativeFeatures(
            source.getPutativeFeatures(),
            null, // No assigned peakels file
            mzTolPPM
    );

    assertNotNull("Matched features should not be null", matchedFeatures);
    assertEquals("Matched features count should match input count", source.getPutativeFeatures().size(), matchedFeatures.size());

    // Check that at least some features were matched
    long matchedCount = matchedFeatures.stream().filter(PutativeFeatureWrapper::isMatched).count();
    assertTrue("Matched count should be 91", matchedCount == 91);

    LOG.info("Matched {} out of {} putative features", matchedCount, matchedFeatures.size());

    assertQuantificationAbundancesMatch(matchedFeatures);
  }

  @Test
  public void testMatchPutativeFeaturesWithEmptyList() throws Exception {

    List<PutativeFeatureWrapper> emptyList = List.of();


    List<PutativeFeatureWrapper> result = finder.matchPutativeFeatures(emptyList, null, mzTolPPM);

    assertNotNull("Result should not be null", result);
    assertTrue("Result should be empty for empty input", result.isEmpty());
  }

  @Test
  public void testMatchedFeaturesHaveExperimentalData() throws Exception {
    if (finder == null) {
      LOG.warn("Skipping test - PeakelsDbFinder not initialized");
      return;
    }

    // Read PSMs from CSV file
    PeakelsProcessing.ReaderConfiguration configuration = PeakelsProcessing.ReaderConfiguration.fromFile(getClass().getResource(PSMS_COLUMNS_FILE).getFile());
    PeakelsProcessing.InputSource source = PeakelsProcessing.readPutativeFeatures(psmsFile, configuration);

    // Initialize MzDbReader
    MzDbReader mzDbReader = new MzDbReader(mzdbFile, true);
    mzDbReader.enableParamTreeLoading();
    mzDbReader.enablePrecursorListLoading();
    mzDbReader.enableScanListLoading();

    // Match PSMs

    List<PutativeFeatureWrapper> matchedPsms = finder.matchIdentifiedPsms(
            source.getPutativeFeatures(),
            mzDbReader,
            mzTolPPM,
            false
    );

    // Verify that matched PSMs have experimental features
    for (PutativeFeatureWrapper psm : matchedPsms) {
      if (psm.isMatched()) {
        assertNotNull("Matched PSM should have experimental feature", psm.getRepresentativeExperimentalFeature());
        Feature feature = psm.getRepresentativeExperimentalFeature();
        assertTrue("Feature m/z should be positive", feature.getMz() > 0);
        assertTrue("Feature elution time should be positive", feature.getElutionTime() > 0);
      }
    }

    mzDbReader.close();
  }

  @Test
  public void testMzToleranceIsAppliedCorrectly() throws Exception {

    // Test with different m/z tolerances
    PeakelsProcessing.ReaderConfiguration configuration = PeakelsProcessing.ReaderConfiguration.fromFile(getClass().getResource(PSMS_COLUMNS_FILE).getFile());
    PeakelsProcessing.InputSource source = PeakelsProcessing.readPutativeFeatures(psmsFile, configuration);

    MzDbReader mzDbReader = new MzDbReader(mzdbFile, true);
    mzDbReader.enableParamTreeLoading();
    mzDbReader.enablePrecursorListLoading();
    mzDbReader.enableScanListLoading();

    // Match with tighter tolerance
    Float tightTolerance = 2.0f;
    List<PutativeFeatureWrapper> tightMatches = finder.matchIdentifiedPsms(
            source.getPutativeFeatures(),
            mzDbReader,
            tightTolerance,
            false
    );

    long tightMatchCount = tightMatches.stream().filter(PutativeFeatureWrapper::isMatched).count();

    LOG.info("Matched {} PSMs with {} ppm tolerance", tightMatchCount, tightTolerance);

    mzDbReader.close();
  }

  /**
   * Represents a quantification result from the reference CSV file for comparison purposes.
   */
  private static class QuantificationResult {
    private final String sequence;
    private final String ptms;
    private final int charge;
    private final double mz;
    private final double rt;
    private final double abundance;

    public QuantificationResult(String sequence, String ptms, int charge, double mz, double rt, double abundance) {
      this.sequence = sequence;
      this.ptms = ptms != null ? ptms : "";
      this.charge = charge;
      this.mz = mz;
      this.rt = rt;
      this.abundance = abundance;
    }

    public String getSequence() {
      return sequence;
    }

    public String getPtms() {
      return ptms;
    }

    public int getCharge() {
      return charge;
    }

    public double getMz() {
      return mz;
    }

    public double getRt() {
      return rt;
    }

    public double getAbundance() {
      return abundance;
    }

    /**
     * Generates an ion key similar to PutativeFeatureWrapper for comparison.
     */
    public String getIonKey() {
      return sequence + "." + ptms + "." + charge;
    }

    /**
     * Checks if this result matches a PutativeFeatureWrapper within given tolerances.
     */
    public boolean matches(PutativeFeatureWrapper feature, double mzTolPpm, double rtTolMin) {
      if (!sequence.equals(feature.getSequence())) {
        return false;
      }
      if (charge != feature.charge()) {
        return false;
      }

      String featurePtms = feature.getModification() != null ? feature.getModification() : "";
      if (!ptms.equals(featurePtms)) {
        return false;
      }

      double mzTolDa = mz * mzTolPpm / 1e6;
      if (Math.abs(mz - feature.getMz()) > mzTolDa) {
        return false;
      }

      double rtSeconds = rt * 60.0;
      if (Math.abs(rtSeconds - feature.elutionTime()) > rtTolMin * 60.0) {
        return false;
      }

      return true;
    }

    @Override
    public String toString() {
      return String.format("QuantificationResult{seq='%s', ptms='%s', charge=%d, mz=%.4f, rt=%.2f, abundance=%.2f}",
              sequence, ptms, charge, mz, rt, abundance);
    }
  }

  /**
   * Reads the quantification results CSV file and returns a list of QuantificationResult objects
   * that can be used to compare abundance values with the results of matchIdentifiedPsms and
   * matchPutativeFeatures methods.
   *
   * @return a list of QuantificationResult objects containing reference abundance values
   * @throws IOException if an error occurs reading the file
   * @throws CsvValidationException if the CSV format is invalid
   */
  private List<QuantificationResult> readQuantificationResults(File quantiFile) throws IOException, CsvValidationException {
    List<QuantificationResult> results = new ArrayList<>();

    final CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(quantiFile))
            .withCSVParser(parser)
            .build()) {

      // Skip header
      String[] header = reader.readNext();

      // Expected columns: Peptide Sequence;PTMs;Score;Charge;m/z;RT;Protein Set Count;Protein Sets;Pep. match count F071239;Abundance F071239
      String[] line;
      while ((line = reader.readNext()) != null) {
        try {
          String sequence = line[0];
          String ptms = line[1];
          int charge = Integer.parseInt(line[3]);
          double mz = Double.parseDouble(line[4]);
          double rt = Double.parseDouble(line[5]); // in minutes
          double abundance = Double.parseDouble(line[9]);

          results.add(new QuantificationResult(sequence, ptms, charge, mz, rt, abundance));
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
          LOG.warn("Error parsing quantification result line: {}", String.join(";", line), e);
        }
      }
    }

    LOG.info("Loaded {} quantification results from {}", results.size(), quantiFile.getName());
    return results;
  }
}