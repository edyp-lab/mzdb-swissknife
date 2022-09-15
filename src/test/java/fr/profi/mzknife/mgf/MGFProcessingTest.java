package fr.profi.mzknife.mgf;

import Preprocessing.Config;
import Preprocessing.DeltaMassDB;
import Preprocessing.JSpectrum;
import fr.profi.mzscope.InvalidMGFFormatException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class MGFProcessingTest {

  private static String srcFilename = "/Xpl1_002790_QX_small.mgf";
  private static String srcTMTFilename = "/TMT_HF1_12692_mzdb_small.mgf";

  private static String destFilename = "unit_test_result.mgf";

  @Test
  public void pCleanTest() {
    File fSrc =new File(MGFProcessingTest.class.getResource(srcFilename).getFile());
    File fDest = new File(fSrc.getParent(),destFilename);

    try {

      PCleanProcessor pCleanProcessor = new PCleanProcessor(fSrc, fDest, "");
      pCleanProcessor.setPCleanParameters(false, false, false, false, false, true, true, false, true);
      JSpectrum.setImmoniumIons();
      DeltaMassDB.consider2aa = true;
      Config.ms2tol = 0.05;

      pCleanProcessor.rewriteMGF();
      testFile(fDest);

    } catch (IOException | InvalidMGFFormatException e) {
      Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for " + srcFilename);
    } finally {
      cleanupFile(fDest);
    }
  }

  @Test
  public void pCleanTMTTest() {
    File fSrc =new File(MGFProcessingTest.class.getResource(srcTMTFilename).getFile());
    File fDest = new File(fSrc.getParent(),destFilename);

    try {

      PCleanProcessor pCleanProcessor = new PCleanProcessor(fSrc, fDest, "TMT11PLEX");
      pCleanProcessor.setPCleanParameters(true, true, true, false, false, true, true, false, true);
      JSpectrum.setImmoniumIons();
      DeltaMassDB.consider2aa = true;
      Config.ms2tol = 0.05;

      pCleanProcessor.rewriteMGF();
      testFile(fDest);

    } catch (IOException | InvalidMGFFormatException e) {
      Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for " + srcFilename);
    } finally {
      cleanupFile(fDest);
    }
  }


  private void cleanupFile(File destFile) {
    if (destFile.exists()) {
      destFile.delete();
    }
  }

  private void testFile(File destFile) {
    Assert.assertTrue(destFile.exists());
  }


}
