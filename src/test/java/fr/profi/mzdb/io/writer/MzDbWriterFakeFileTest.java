package fr.profi.mzdb.io.writer;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.BBSizes;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.*;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.db.model.params.param.CV;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.CVTerm;
import fr.profi.mzdb.db.model.params.param.CVUnit;
import fr.profi.mzdb.model.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class MzDbWriterFakeFileTest {

  public static final float RT_FREQUENCY = 10.0f;

  static class Peak {
    double mz;
    double intensity;

    Peak(double mz) {
      this.mz = mz;
      this.intensity = 0.0;
    }

    float getIntensity() {
      return (float)intensity;
    }

    float getFwhm() {
      return (float)(5.0*mz/1e6);
    }


   }

  private static String filename = "Fake.mzdb";
  private static String destFilename = ".\\wrong_NEW_"+filename;
  private static final BBSizes BB_SIZES = new BBSizes(5, 10000, 15, 15);
  private static DataEncoding FITTED_ENCODING = new DataEncoding(-1,DataMode.FITTED, PeakEncoding.HIGH_RES_PEAK, "none",ByteOrder.LITTLE_ENDIAN);

  private static final AcquisitionMode ACQUISITION_MODE = AcquisitionMode.DDA;

  private static Random R = new Random();
  private static final Peak[] PEAKS = { new Peak(352.0), new Peak(647.88), new Peak(649.2), new Peak(698.35), new Peak(783.56), new Peak(784.01), new Peak(902.2), new Peak(1215.0) };

  @Test
  public void generateFakeMS1Mzdb() {

    MzDBWriter mzDbWriter = null;
    File fDest =new File(destFilename);

    if(fDest.exists())
      fDest.delete();

    try {

      MzDBMetaData metaData = createFakeMzDbMetaData();
      mzDbWriter = new MzDBWriter(fDest, metaData, BB_SIZES,false);
      mzDbWriter.initialize();

      for(int i = 1 ; i <= 200 ; i++) {
        final List<Peak> peakList = new ArrayList<>();
        peakList.addAll(Arrays.asList(PEAKS));

        // simulate a shorter mz range for the first scan
        if (i == 1) peakList.set(0, null);

        Spectrum sp = createFakeMS1Spectrum(i, peakList);
        SpectrumHeader sh = sp.getHeader();
        SpectrumMetaData smd = new SpectrumMetaData(sh.getId(), "<params/>", null, null);
        mzDbWriter.insertSpectrum(sp, smd, FITTED_ENCODING);
      }
    } catch (SQLiteException e) {
      e.printStackTrace();
      Assert.fail("MzDB writer exception " + e.getMessage() );
    } finally {
      if(mzDbWriter != null) {
        mzDbWriter.close();
        mzDbWriter.dumpExecutionWatches();
      }
    }

  testFile(fDest);

  }

  private Spectrum createFakeMS1Spectrum(int i, List<Peak> peaks) {

    final List<Peak> peakList = peaks.stream().filter(p -> p != null).collect(Collectors.toList());

    double[] masses = new double[peakList.size()];
    float[] leftFwhm = new float[peakList.size()];
    float[] rightFwhm = new float[peakList.size()];
    float[] intensities = new float[peakList.size()];

    peakList.stream().forEach(p -> p.intensity = 10000.0 + 10000.0* R.nextDouble());

    for(int k = 0; k < peakList.size(); k++) {
      final Peak peak = peakList.get(k);
      if (peak != null) {
        masses[k] = peak.mz;
        intensities[k] = peak.getIntensity();
        rightFwhm[k] = peak.getFwhm();
        leftFwhm[k] = peak.getFwhm();
      }
    }

    float tic = (float) peakList.stream().mapToDouble(p -> p.intensity).sum();
    SpectrumData data = new SpectrumData(masses, intensities, leftFwhm, rightFwhm);
    Optional<Peak> basePeak = peakList.stream().max(Comparator.comparingDouble(p -> p.intensity));
    SpectrumHeader header = new SpectrumHeader(i, i, i, i/ RT_FREQUENCY, 1, "Title spectrum number "+i, peakList.size(), true, tic, basePeak.get().mz, basePeak.get().getIntensity(), null, null, -1, ActivationType.CID);
    Spectrum spectrum = new Spectrum(header, data);

    return spectrum;
  }

  public MzDBMetaData createFakeMzDbMetaData() throws SQLiteException {

    MzDbHeader mzdbHeader = new MzDbHeader(Double.toString(MzDBWriter.MODEL_VERSION), (int)Timestamp.from(Instant.now()).getTime());

    List<DataEncoding> dataEncodings = new ArrayList<>(Arrays.asList(FITTED_ENCODING));
    List<InstrumentConfiguration> instrumentConfigurations = new ArrayList<>();

    // Processing methods ... except that there is no api in mzdb-access to read Processing methods. TODO !!
    List<ProcessingMethod> processingMethods = new ArrayList<>();
    int processingMethodsNumber = 0;
    int processingMethodsId = 1;

    ParamTree paramTree = new ParamTree();
    Software software = new Software(-1, "fakemzDB", "0.9.10", paramTree);
    List<Software> softwareList = new ArrayList<>();
    softwareList.add(software);

    for (Software srcSoftware : softwareList) {
      ProcessingMethod pm = new ProcessingMethod(processingMethodsId++, null, processingMethodsNumber++, "fake processing method - to be done", (int) srcSoftware.getId());
      processingMethods.add(pm);
    }

    List<Run> runs = new ArrayList<>();
    paramTree = new ParamTree();
    List<CVParam> cvParamsList = new ArrayList<>();
    cvParamsList.add(buildCVParam("MS:1001954", "acquisition parameter", "DDA acquisition", "MS"));
    paramTree.setCvParams(cvParamsList);
    Run run = new Run(-1, "FakemzDB", Date.from(Instant.now()), paramTree);
    runs.add(run);
    List<Sample> samples = new ArrayList<>();
    List<SourceFile> sourceFiles =new ArrayList<>();
    List<SharedParamTree> sharedParamTrees = new ArrayList<>();
    List<CV> cvs = new ArrayList<>();
    List<CVTerm> cvTerms = new ArrayList<>();
    List<CVUnit> cvUnits = new ArrayList<>();

    MzDBMetaData mzMetaData = new MzDBMetaData();
    mzMetaData.setMzdbHeader(mzdbHeader);
    mzMetaData.setDataEncodings(dataEncodings);
    mzMetaData.setSharedParamTrees(sharedParamTrees);
    mzMetaData.setInstrumentConfigurations(instrumentConfigurations);
    mzMetaData.setProcessingMethods(processingMethods);
    mzMetaData.setRuns(runs);
    mzMetaData.setSamples(samples);
    mzMetaData.setSourceFiles(sourceFiles);
    mzMetaData.setSoftwares(softwareList);
    mzMetaData.setCvList(cvs);
    mzMetaData.setCvTerms(cvTerms);
    mzMetaData.setCvUnits(cvUnits);

    return mzMetaData;
  }

  private CVParam buildCVParam(String accession, String name, String value, String cvRef) {
    CVParam param = new CVParam();
    param.setAccession(accession);
    param.setName(name);
    param.setValue(value);
    param.setCvRef(cvRef);
    return param;
  }

  public void testFile(File file) {
    MzDbReader mzDb = null;
    // create Reader
    try {
      mzDb = new MzDbReader(file, true);

    } catch ( FileNotFoundException | SQLiteException e) {
      e.printStackTrace();
      Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for " + filename);
    }
    Assert.assertNotNull("Reader cannot be created", mzDb);
    System.out.print(".");
  }

}
