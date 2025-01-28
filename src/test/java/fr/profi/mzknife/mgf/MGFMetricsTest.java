package fr.profi.mzknife.mgf;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class MGFMetricsTest {

  private static String srcFilename = "/Xpl1_002790_QX_small.mgf";

  @Test
  public void testMGFMetrics() throws Exception {

    String srcFilename = "/Xpl1_002790_QX_small.mgf";
    File fSrc =new File(MGFProcessingTest.class.getResource("/Xpl1_002790_QX_small.mgf").getFile());
    File fDest = new File(fSrc.getParent(),"output.tsv");

    MGFMetrics metrics = new MGFMetrics(fSrc, fDest);
    metrics.dumpMGFMetrics();

    Assert.assertTrue(fDest.exists());
  }

  private void cleanupFile(File destFile) {
    if (destFile.exists()) {
      destFile.delete();
    }
  }


}
