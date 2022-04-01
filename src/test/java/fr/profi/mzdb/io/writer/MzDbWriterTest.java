package fr.profi.mzdb.io.writer;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.MzDbMetaData;
import fr.profi.mzdb.db.model.SpectrumMetaData;
import fr.profi.mzdb.io.writer.MzDBWriter;
import fr.profi.mzdb.io.writer.ParamTreeStringifier;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.Spectrum;
import fr.profi.mzdb.model.SpectrumHeader;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.Map;

public class MzDbWriterTest {

  private static String srcFilename = "/OVEMB150205_12.raw.0.9.8.mzDB";
  private static String destFilename = "New_OVEMB150205_12.raw.0.9.8.mzDB";
//private static String srcFilename = "C:\\vero\\DEV\\Proline\\mzdb\\frag\\QEx2_020038.mzdb";
//private static String destFilename = "C:\\vero\\DEV\\Proline\\mzdb\\frag\\NewQEx2_020038.mzdb";

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
      Assert.assertNotNull("Reader cannot be created", mzDb);
    } catch (ClassNotFoundException | FileNotFoundException | SQLiteException e) {
      Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for " + srcFilename);
    }

    MzDbMetaData metaData = new MzDbMetaData();
    try {
      metaData.setMzdbHeader(mzDb.getMzDbHeader());
      metaData.setDataEncodings(new ArrayList<>(mzDb.getDataEncodingBySpectrumId().values()));
      metaData.setInstrumentConfigurations(mzDb.getInstrumentConfigurations());
      metaData.setRuns(mzDb.getRuns());
      metaData.setSamples(mzDb.getSamples());
      metaData.setSoftwares(mzDb.getSoftwareList());
      metaData.setSourceFiles(mzDb.getSourceFiles());
      mzDbWriter = new MzDBWriter(fDest, metaData, mzDb.getBBSizes(),false);
      mzDbWriter.initialize();

      int c = mzDb.getSpectraCount();
      Map<Long, DataEncoding> deBySpId = mzDb.getDataEncodingBySpectrumId();
      for(int i=1 ; i<=c;i++) {
        Spectrum sp = mzDb.getSpectrum(i);
        SpectrumHeader sh1 =  mzDb.getSpectrumHeader(i);
        SpectrumHeader sh = sp.getHeader();
        String paramTree = sh.hasParamTree() ? ParamTreeStringifier.stringifyParamTree(sh.getParamTree(mzDb.getConnection())) : null;
        SpectrumMetaData smd = new SpectrumMetaData(sh.getId(), paramTree,sh.getScanListAsString(mzDb.getConnection()),sh.getPrecursorListAsString(mzDb.getConnection())  );

        mzDbWriter.insertSpectrum(sp, smd, deBySpId.get(sh.getId()));
      }
    } catch (SQLiteException | StreamCorruptedException e) {
      e.printStackTrace();
    } finally {
      mzDb.close();
      if(mzDbWriter != null)
        mzDbWriter.close();
    }

  }
}
