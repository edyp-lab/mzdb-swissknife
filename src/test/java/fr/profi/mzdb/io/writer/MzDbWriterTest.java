package fr.profi.mzdb.io.writer;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.BBSizes;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.*;
import fr.profi.mzdb.db.model.params.param.*;
import fr.profi.mzdb.io.reader.iterator.SpectrumIterator;
import fr.profi.mzdb.io.util.MzDBUtil;
import fr.profi.mzdb.model.*;
import fr.profi.mzdb.model.MzDBMetaData;
import fr.profi.mzdb.model.SpectrumMetaData;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StreamCorruptedException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MzDbWriterTest {

  private static String srcFilename = "/OVEMB150205_12.raw.0.9.8.mzDB";
  private static String destFilename = "New_OVEMB150205_12.raw.0.9.8.mzDB";
//private static String srcFilename = "C:\\vero\\DEV\\Proline\\mzdb\\frag\\QEx2_020038.mzdb";
//private static String destFilename = "C:\\vero\\DEV\\Proline\\mzdb\\frag\\NewQEx2_020038.mzdb";

  // VDS : Voir pour heriter de MzDbReaderTest ...
  private static final float FLOAT_EPSILON = 1E-4f;
  private static final BBSizes expectedBBSizes_OVEMB150205_12 = new BBSizes(5, 10000, 15, 15);
//  private static final int expectedBBCount_OVEMB150205_12 = 3406;
  private static final int expectedBBCount_NEW_OVEMB150205_12 = 2402; //Prev value was 2387;
  private static final int expectedCycleCount_OVEMB150205_12 = 158;
  private static final int expectedRunSliceCount_OVEMB150205_12 = 241; //Prev value was 161;
  private static final int expectedSpectrumCount_OVEMB150205_12 = 1193;
  private static final int expectedDataEncodingCount_NEW_OVEMB150205_12 = 2;
  private static final int expectedMaxMSLevel_OVEMB150205_12 = 2;
  private static final float expectedLastRTTime_OVEMB150205_12 = 240.8635f;
  private static final IsolationWindow[] expectedDiaIsolationWindows_OVEMB150205_12 = {};
  private static final String expectedSharedParam_CVMS1001742_ACCESSION="MS:1001742";
  private static final String expectedSharedParam_CVMS1001742_NAME ="LTQ Orbitrap Velos";
  private static final String expectedModelVersion_OVEMB150205_12_0_9_8 = "0.7";

  private static final String expectedCV_ID = "psi_ms";
  private static final String expectedCVUNIT_3_ACCESSION = "MS:1000131";
  private static final int expectedCvParamsCount_OVEMB150205_12__0_9_8 = 1;
  private static final float minMz_OVEMB150205_12 = 400f;
  private static final float maxMz_OVEMB150205_12 = 600f;
  private static final float minRt_OVEMB150205_12 = 100f;
  private static final float maxRt_OVEMB150205_12 = 200f;
  private static final int expectedSpectrumSlicesCount_OVEMB150205_12 = 63;
//  private static final double expectedSumIntensities_OVEMB150205_12__0_9_7 = 2.543672190435547E9;
  private static final double expectedSumIntensities_OVEMB150205_12__0_9_8 = 2.570950375147949E9; //Prev value was 2.5717392830078125E9;
//  private static final double expectedSumMz_OVEMB150205_12__0_9_7 = 3.868285366432487E7;
  private static final double expectedSumMz_OVEMB150205_12__0_9_8 = 3.864962804092407E7; //Prev value was 3.867483975354004E7;
//  private static final int expectedNbIntensities_OVEMB150205_12__0_9_7 = 155874;
  private static final int expectedNbIntensities_OVEMB150205_12__0_9_8 = 155712; //Prev value was 155838;
//  private static final int expectedNbPeaks_OVEMB150205_12__0_9_7 = 0;
  private static final int expectedNbPeaks_OVEMB150205_12__0_9_8 = 0;
  private static final AcquisitionMode expectedAcquisitionMode_OVEMB150205_12__0_9_8 = AcquisitionMode.DDA;

  @Test
  public void testReadWrite(){
    MzDbReader mzDb = null;
    MzDBWriter mzDbWriter = null;

    File fSrc =new File(MzDbWriterTest.class.getResource(srcFilename).getFile());
    File fDest = new File(fSrc.getParent(),destFilename);
//    File fSrc =new File(srcFilename);
//    File fDest =new File(destFilename);
    if(fDest.exists())
      fDest.delete();

    System.out.print("Read mzDB file " + srcFilename + " and Write it back to "+destFilename);
    try {
      mzDb = new MzDbReader(fSrc, true);
      mzDb.enableScanListLoading();
      mzDb.enableParamTreeLoading();
      mzDb.enablePrecursorListLoading();
      mzDb.enableDataStringCache();
      Assert.assertNotNull("Reader cannot be created", mzDb);
    } catch (ClassNotFoundException | FileNotFoundException | SQLiteException e) {
      Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for " + srcFilename);
    }

    try {

      MzDBMetaData metaData = MzDBUtil.createMzDbMetaData(mzDb);
      mzDbWriter = new MzDBWriter(fDest, metaData, mzDb.getBBSizes(),false);
      mzDbWriter.initialize();

      int c = mzDb.getSpectraCount();
      Assert.assertEquals(expectedSpectrumCount_OVEMB150205_12, c);

      Map<Long, DataEncoding> deBySpId = mzDb.getDataEncodingBySpectrumId();
      for(int i=1 ; i<=c;i++) {
        Spectrum sp = mzDb.getSpectrum(i);
        SpectrumHeader sh = sp.getHeader();
        String paramTree = sh.hasParamTree() ? ParamTreeStringifier.stringifyParamTree(sh.getParamTree(mzDb.getConnection())) : null;
        SpectrumMetaData smd = new SpectrumMetaData(sh.getId(), paramTree,sh.getScanListAsString(mzDb.getConnection()),sh.getPrecursorListAsString(mzDb.getConnection())  );
        mzDbWriter.insertSpectrum(sp, smd, deBySpId.get(sh.getId()));
      }
    } catch (SQLiteException | StreamCorruptedException e) {
      e.printStackTrace();
      Assert.fail("MzDB writer exception " + e.getMessage() );
    } finally {
      mzDb.close();
      if(mzDbWriter != null)
        mzDbWriter.close();
    }

    //read back mzdb file
    testFile(fDest, expectedModelVersion_OVEMB150205_12_0_9_8, expectedSumIntensities_OVEMB150205_12__0_9_8, expectedSumMz_OVEMB150205_12__0_9_8,
            expectedNbIntensities_OVEMB150205_12__0_9_8, expectedNbPeaks_OVEMB150205_12__0_9_8, expectedCvParamsCount_OVEMB150205_12__0_9_8, expectedAcquisitionMode_OVEMB150205_12__0_9_8);
  }

  public void testFile(File file, String expectedModelVersion, double expectedSumIntensities,
                         double expectedSumMz, int expectedNbIntensities, int expectedNbPeaks, int expectedCvParamsCount, AcquisitionMode expectedAcquisitionMode) {
    MzDbReader mzDb = null;
    String filename = file.getName();

    System.out.print("Non Regression test reading mzDB file " + filename + ": ");
    // create Reader
    try {
      mzDb = new MzDbReader(file, true);

    } catch (ClassNotFoundException | FileNotFoundException | SQLiteException e) {
      Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for " + filename);
    }
    Assert.assertNotNull("Reader cannot be created", mzDb);
    System.out.print(".");

    //Read SharedParamTree
    try {
      List<SharedParamTree> sharedParamTrees = mzDb.getSharedParamTreeList();
      Assert.assertEquals(1, sharedParamTrees.size());

      List<CVParam> params = sharedParamTrees.get(0).getData().getCVParams();
      boolean found = false;
      String cvName ="";
      for(CVParam p : params){
        if (p.getAccession().equals(expectedSharedParam_CVMS1001742_ACCESSION)) {
          found = true;
          cvName = p.getName();
          break;
        }
      }

      Assert.assertTrue(found);
      Assert.assertEquals(expectedSharedParam_CVMS1001742_NAME, cvName);

    } catch (SQLiteException e) {
      Assert.fail("SharedParamTree exception " + e.getMessage() + " for " + filename);
    }


    //Read CV data
    try {
      List<CV> cVs = mzDb.getCvList();
      Assert.assertEquals(1, cVs.size());

      String id = cVs.get(0).getCvId();
      Assert.assertEquals(expectedCV_ID, id);


      List<CVUnit> units = mzDb.getCvUnitList();
      Assert.assertEquals(5, units.size());

      String accession = units.get(3).getAccession();
      Assert.assertEquals(expectedCVUNIT_3_ACCESSION, accession);
      System.out.print(".");
    } catch (SQLiteException e) {
      Assert.fail("SharedParamTree exception " + e.getMessage() + " for " + filename);
    }

    // Bounding boxes size
    try {
      BBSizes bbSizes = mzDb.getBBSizes();
      Assert.assertEquals("BBSize " + filename + " invalid", expectedBBSizes_OVEMB150205_12, bbSizes);
    } catch (SQLiteException e) {
      Assert.fail("BBSizes exception " + e.getMessage() + " for " + filename);
    }
    System.out.print(".");

    // Bounding boxes count
    try {
      int bbCount = mzDb.getBoundingBoxesCount();
      Assert.assertEquals("BBCount " + filename + " invalid", expectedBBCount_NEW_OVEMB150205_12, bbCount);
    } catch (SQLiteException e) {
      Assert.fail("BBCount exception " + e.getMessage() + " for " + filename);
    }
    System.out.print(".");

    // Cycle count
    try {
      int cycleCount = mzDb.getCyclesCount();
      Assert.assertEquals("CycleCount " + filename + " invalid", expectedCycleCount_OVEMB150205_12,
              cycleCount);
    } catch (SQLiteException e) {
      Assert.fail("CycleCount exception " + e.getMessage() + " for " + filename);
    }

    // Cycle count
    try {
      int runSliceCount = mzDb.getRunSlicesCount();
      Assert.assertEquals("RunSliceCount " + filename + " invalid",
              expectedRunSliceCount_OVEMB150205_12, runSliceCount);
    } catch (SQLiteException e) {
      Assert.fail("RunSliceCount exception " + e.getMessage() + " for " + filename);
    }
    System.out.print(".");

    // Spectrum count
    try {
      int spectrumCount = mzDb.getSpectraCount();
      Assert.assertEquals("SpectrumCount " + filename + " invalid", expectedSpectrumCount_OVEMB150205_12,
              spectrumCount);
    } catch (SQLiteException e) {
      Assert.fail("SpectrumCount exception " + e.getMessage() + " for " + filename);
    }
    System.out.print(".");

    // Data Encoding count
    try {
      int dataEncodingCount = mzDb.getDataEncodingsCount();
      Assert.assertEquals("DataEncodingCount " + filename + " invalid",
              expectedDataEncodingCount_NEW_OVEMB150205_12, dataEncodingCount);
    } catch (SQLiteException e) {
      Assert.fail("DataEncodingCount exception " + e.getMessage() + " for " + filename);
    }
    System.out.print(".");

    // Max MS Level
    try {
      int maxMSLevel = mzDb.getMaxMsLevel();
      Assert.assertEquals("MaxMSLevel " + filename + " invalid", expectedMaxMSLevel_OVEMB150205_12,
              maxMSLevel);
    } catch (SQLiteException e) {
      Assert.fail("MaxMSLevel exception " + e.getMessage() + " for " + filename);
    }
    System.out.print(".");

    // Max MS Level
    try {
      float lastRTTime = mzDb.getLastTime();
      Assert.assertEquals("lastRTTime " + filename + " invalid", expectedLastRTTime_OVEMB150205_12,
              lastRTTime, FLOAT_EPSILON);
    } catch (SQLiteException e) {
      Assert.fail("lastRTTime exception " + e.getMessage() + " for " + filename);
    }
    System.out.print(".");

    // read Model Version
    try {
      String modelVersion = mzDb.getModelVersion();
      Assert.assertEquals("ModelVersion " + filename + " invalid", expectedModelVersion, modelVersion);
    } catch (SQLiteException e) {
      Assert.fail("version exception " + e.getMessage() + " for " + filename);
    }
    System.out.print(".");

    // read Acquisition Mode
    try {
      AcquisitionMode acquisitionMode = mzDb.getAcquisitionMode();
      Assert.assertEquals("AcquisitionMode " + filename + " invalid",
              expectedAcquisitionMode, acquisitionMode);
    } catch (SQLiteException e) {
      Assert.fail("version exception " + e.getMessage() + " for " + filename);
    }
    System.out.print(".");

    // read DIA Isolation Window
    // FIXME: test has
    // try {
    // IsolationWindow[] diaIsolationWindows = mzDb.getDIAIsolationWindows();
    // // System.out.println(diaIsolationWindows.length);
    // // for (IsolationWindow w : diaIsolationWindows) {
    // // System.out.println("-------------------------------------------");
    // // System.out.println(w.getMinMz());
    // // System.out.println(w.getMaxMz());
    // // }
    // Assert.assertArrayEquals("AcquisitionMode " + filename + " invalid", new IsolationWindow[] {},
    // diaIsolationWindows);
    // } catch (SQLiteException e) {
    // Assert.fail("version exception " + e.getMessage() + " for " + filename);
    // }
    System.out.print(".");

    try {
      SpectrumSlice[] spectrumSlices = mzDb.getMsSpectrumSlices(minMz_OVEMB150205_12, maxMz_OVEMB150205_12,
              minRt_OVEMB150205_12, maxRt_OVEMB150205_12);
      Assert.assertNotNull(spectrumSlices);
      Assert.assertEquals(expectedSpectrumSlicesCount_OVEMB150205_12, spectrumSlices.length);
      int nbIntensities = 0;
      int nbPeaks = 0;
      double sumIntensities = 0;
      double sumMz = 0;
      for (SpectrumSlice spectrumSlice : spectrumSlices) {
        for (double intensity : spectrumSlice.getData().getIntensityList()) {
          sumIntensities += intensity;
        }
        for (double mz : spectrumSlice.getData().getMzList()) {
          sumMz += mz;
        }
        nbIntensities += spectrumSlice.getData().getIntensityList().length;
        nbIntensities += spectrumSlice.getData().getPeaksCount();
      }
      Assert.assertEquals(expectedSumIntensities, sumIntensities, 1);
      Assert.assertEquals(expectedSumMz, sumMz, 1E-2);
      Assert.assertEquals(expectedNbIntensities, nbIntensities);
      Assert.assertEquals(expectedNbPeaks, nbPeaks);
    } catch (StreamCorruptedException | SQLiteException e1) {
      Assert.fail("spectrum slices extraction throws exception " + e1.getMessage());
    }
    // read Isolation Window
    try {
      List<Run> runs = mzDb.getRuns();
      Assert.assertEquals(1, runs.size());
      List<Sample> samples = mzDb.getSamples();
      Assert.assertEquals(1, samples.size());
      // System.out.println(diaIsolationWindows.length);
      for (Run run : runs) {
        Assert.assertEquals("OVEMB150205_12", run.getName());
        Assert.assertEquals(1, run.getId());
        List<CVParam> cvParams = run.getCVParams();
        Assert.assertEquals(expectedCvParamsCount, cvParams.size());
        List<UserParam> userParams = run.getUserParams();
        Assert.assertEquals(0, userParams.size());
        List<UserText> userText = run.getUserTexts();
        Assert.assertEquals(0, userText.size());
      }
      Assert.assertEquals("UPS1 5fmol R1", samples.get(0).getName());

      try {
        Iterator<Spectrum> iterator = new SpectrumIterator(mzDb, mzDb.getConnection(), 1);
        int spectrumIndex = 0;
        while (iterator.hasNext()) {
          Spectrum spectrum = iterator.next();
          SpectrumData data = spectrum.getData();
          int s = data.getIntensityList().length;
          Assert.assertEquals(s, data.getIntensityList().length);
          Assert.assertEquals(s, data.getMzList().length);
          Assert.assertEquals(s, data.getLeftHwhmList().length);
          Assert.assertEquals(s, data.getRightHwhmList().length);
          spectrumIndex++;
        }
        Assert.assertEquals(expectedCycleCount_OVEMB150205_12, spectrumIndex);
      } catch (StreamCorruptedException e) {
        e.printStackTrace();
      }

    } catch (SQLiteException e) {
      Assert.fail("version exception " + e.getMessage() + " for " + filename);
    }
    System.out.print(".");

  }
}
