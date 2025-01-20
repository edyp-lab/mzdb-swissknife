package fr.profi.mzknife.mgf;

import Preprocessing.Config;
import Preprocessing.DeltaMassDB;
import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.Spectrum;
import fr.profi.mzscope.MGFConstants;
import fr.profi.mzscope.MSMSSpectrum;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;

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

  @Test
  public void cleaningTest() {

    String srcFilename = "C:/Local/bruley/Tests/MGF/Data/mzdb-converter-1.2.1/Xpl1_002787.mzdb";
    String destFilename = "cleaning_unit_test_result.mgf";

    File fDest = new File((new File(srcFilename)).getParent(),destFilename);

    try {

      PrintWriter mgfWriter = new PrintWriter(new BufferedWriter(new FileWriter(fDest)));

      MzDbReader mzDb = new MzDbReader(srcFilename, true);

      mzDb.enablePrecursorListLoading();

      MSMSSpectrum spectrum = toMSMSSpectrum(mzDb.getSpectrum(67806));

      PCleanProcessor pCleanProcessor = new PCleanProcessor("");
      DeltaMassDB.consider2aa = true;
      Config.ms2tol = PCleanProcessor.MS2_DEFAULT_TOL;

      MSMSSpectrum cleanedSpectrum = pCleanProcessor.getSpectrum2Export(spectrum);
      cleanedSpectrum.setAnnotation(MGFConstants.TITLE, "pClean");
      String spectrumAsStr = MGFWriter.stringifySpectrum(cleanedSpectrum);
      mgfWriter.print(spectrumAsStr);
      mgfWriter.println();

      MGFECleaner cleaner = new MGFECleaner(20.0);
      cleanedSpectrum = cleaner.getSpectrum2Export(spectrum);
      cleanedSpectrum.setAnnotation(MGFConstants.TITLE, "clean");

      spectrumAsStr = MGFWriter.stringifySpectrum(cleanedSpectrum);
      mgfWriter.print(spectrumAsStr);
      mgfWriter.println();


      mgfWriter.flush();
      mgfWriter.close();

      mzDb.close();

    } catch (FileNotFoundException | SQLiteException e) {
      throw new RuntimeException(e);
    } catch (StreamCorruptedException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
//      cleanupFile(fDest);
    }
  }

  private MSMSSpectrum toMSMSSpectrum(Spectrum spectrum) {
    MSMSSpectrum ms2Spectrum = new MSMSSpectrum(spectrum.getHeader().getPrecursorMz(), spectrum.getHeader().getTIC(), spectrum.getHeader().getPrecursorCharge(), spectrum.getHeader().getElutionTime());
    final double[] mzList = spectrum.getData().getMzList();
    final float[] intensityList = spectrum.getData().getIntensityList();
    for(int k = 0; k < mzList.length; k++) {
      ms2Spectrum.addPeak(mzList[k], intensityList[k]);
    }
    ms2Spectrum.setAnnotation(MGFConstants.SCANS, spectrum.getHeader().getSpectrumId());
    return ms2Spectrum;

  }

  private void cleanupFile(File destFile) {
    if (destFile.exists()) {
      destFile.delete();
    }
  }


}
