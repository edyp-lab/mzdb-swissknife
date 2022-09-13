package fr.profi.mzknife.mzdb;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.writer.mgf.*;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.Spectrum;
import fr.profi.mzknife.mgf.PCleanProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.Map;

public class MzDbProcessingTest {

  private static String srcFilename = "/OVEMB150205_12.raw.0.9.8.mzDB";
  private static String destFilename = "New_OVEMB150205_12.raw.0.9.8.mgf";


  @Test
  public void testCreateMgf(){

    File fSrc =new File(MzDbProcessingTest.class.getResource(srcFilename).getFile());
    File fDest = new File(fSrc.getParent(),destFilename);

    if(fDest.exists())
      fDest.delete();

    try {

      MgfWriter writer = new MgfWriter(fSrc.getAbsolutePath(), 2);

      IPrecursorComputation precComputer = new IsolationWindowPrecursorExtractor(10.0f);
      writer.write(fDest.getAbsolutePath(), precComputer, new DefaultSpectrumProcessor(), 0.0f, true);

      testFile(fDest);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("MzDB writer exception " + e.getMessage() );
    } finally {
      cleanupFile(fDest);
    }

    }

  @Test
  public void testCreateMgf_v3_6(){

    File fSrc =new File(MzDbProcessingTest.class.getResource(srcFilename).getFile());
    File fDest = new File(fSrc.getParent(),destFilename);

    if(fDest.exists())
      fDest.delete();

    try {

      MgfWriter writer = new MgfWriter(fSrc.getAbsolutePath(), 2);

      IPrecursorComputation precComputer = new IsolationWindowPrecursorExtractor_v3_6(10.0f);
      writer.write(fDest.getAbsolutePath(), precComputer, new DefaultSpectrumProcessor(), 0.0f, true);

      testFile(fDest);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("MzDB writer exception " + e.getMessage() );
    } finally {
      cleanupFile(fDest);
    }
  }

  @Test
  public void testCreateMgf_v3_6_pclean() {

    File fSrc =new File(MzDbProcessingTest.class.getResource(srcFilename).getFile());
    File fDest = new File(fSrc.getParent(),destFilename);

    if(fDest.exists())
      fDest.delete();

    try {

      MgfWriter writer = new MgfWriter(fSrc.getAbsolutePath(), 2);

      IPrecursorComputation precComputer = new IsolationWindowPrecursorExtractor_v3_6(10.0f);
      writer.write(fDest.getAbsolutePath(), precComputer, new PCleanProcessor(""), 0.0f, true);

      testFile(fDest);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("MzDB writer exception " + e.getMessage() );
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

