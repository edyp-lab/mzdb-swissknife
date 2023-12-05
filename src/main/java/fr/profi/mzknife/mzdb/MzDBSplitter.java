package fr.profi.mzknife.mzdb;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.BBSizes;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.SharedParamTree;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.UserText;
import fr.profi.mzdb.io.util.MzDBUtil;
import fr.profi.mzdb.io.writer.MzDBWriter;
import fr.profi.mzdb.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StreamCorruptedException;
import java.util.*;

public class MzDBSplitter {

  File m_inputMzdbFile;
  List<File> m_outputMzdbFiles;
  MzDbReader m_mzDbReader;

  String m_fileExtension = ".mzdb";

 public enum RETURN_CODE {
     UNDEFINED, OK, NOT_EXPLORIS, NO_CVS, EXCEPTION;
  }

  private RETURN_CODE m_finishCode = RETURN_CODE.UNDEFINED;

  private final static String USER_TEXT_TAG = "instrumentMethods";
  private final static String FAIMS_CV_PREFIX = "FAIMS CV";
  private final static String CV_PARAM_ACC  = "MS:1001581"; // FAIMS compensation voltage CVParam
  private final static Logger LOG = LoggerFactory.getLogger(MzDBSplitter.class);

  public MzDBSplitter(File inputMzdbFile) {
    this.m_inputMzdbFile = inputMzdbFile;
    initReader();
  }

  private void initReader() {
    try {
      m_mzDbReader = new MzDbReader(m_inputMzdbFile, true);
      m_mzDbReader.enableScanListLoading();
      m_mzDbReader.enableParamTreeLoading();
      m_mzDbReader.enablePrecursorListLoading();
      m_mzDbReader.enableDataStringCache();
    } catch (SQLiteException | FileNotFoundException e) {
      e.printStackTrace();
      if(m_mzDbReader != null)
        m_mzDbReader.close();
      throw new IllegalArgumentException("Unable to read specified mzDbFile");
    }
  }

  private File getOutFile(String prefix) {
    String rootFileName = m_inputMzdbFile.getName();
    int index = rootFileName.lastIndexOf('.');
    if (index > 0)
      rootFileName = rootFileName.substring(0, index);
    rootFileName = rootFileName + prefix.trim() + m_fileExtension;
    return new File(m_inputMzdbFile.getParentFile(), rootFileName);
  }

  public void  setOutputFileExtension(String extension){
    m_fileExtension = extension;
  }


  public List<File> getOutputMzdbFiles() {
    return m_outputMzdbFiles;
  }

  public RETURN_CODE getFinishStateCode(){
    return m_finishCode;
  }

  public boolean splitMzDbFile() {

    LOG.info(" Split mzDB files "+m_inputMzdbFile.getName());
    Map<String, MzDBWriter> writerPerCV = new HashMap<>();
    BBSizes defaultBBsize = new BBSizes(5, 10000, 15, 0);
    try {

      if(!m_mzDbReader.getConnection().isOpen())
        initReader();

      SpectrumHeader[] headers = m_mzDbReader.getSpectrumHeaders();
      Arrays.sort(headers, Comparator.comparingLong(SpectrumHeader::getSpectrumId));

      //To know if mzdb is issu from an exploris !
      // 1. table shared_param_tree ; col  :data
      //<referenceableParamGroup id="CommonInstrumentParams">
      //  <cvParam cvRef="MS" accession="MS:1003028" name="Orbitrap Exploris 480" value=""/>
      // 2. table mzdb ; col : param tree
      // <userText cvRef="MS" accession="MS:-1" name="instrumentMethods" type="xsd:string">
      // ...
      // Instrument: U3000Nano on exploris-480
      //...
      //Global Settings
      // ...
      //   FAIMS Gas = 0
      //   FAIMS Mode = Standard Resolution
      //...
      //Experiment 1
      //   Experiment Name = -50 ...
      //   FAIMS CV = -50
      // ..
      //Experiment 1
      //   Experiment Name =  -70 ...
      //   FAIMS CV = -70
      // ..
      List<SharedParamTree> sharedParamTrees = m_mzDbReader.getSharedParamTreeList();
      boolean isSplittable = false;
      for (SharedParamTree sharedParamTree : sharedParamTrees) {
        List<CVParam> params = sharedParamTree.getData().getCVParams();
        for (CVParam p : params) {
          if (p.getName().toLowerCase().contains("exploris")) {
            isSplittable = true;
            break;
          }
        }
      }
      LOG.trace(" is mzDB file Shared Param Tree 'Exploris'? "+isSplittable);

      int nbrCV = 0;
      List<String> cvPrefix = new ArrayList<>();
      List<UserText> userParams = m_mzDbReader.getMzDbHeader().getUserTexts();
      if (userParams != null && !userParams.isEmpty()) {
        for (UserText userText : userParams) {
          if (USER_TEXT_TAG.equals(userText.getName())) {
            String instrumMethods = userText.getText();
            Scanner scanner = new Scanner(instrumMethods);
            while (scanner.hasNextLine()) {
              String line = scanner.nextLine().trim();
              if (line.startsWith(FAIMS_CV_PREFIX)) { //Got a CV
                isSplittable = true; // if not found in SharedParamTree
                nbrCV++;
                String cvName = line.substring(8).trim();
                if (cvName.startsWith("="))
                  cvName = cvName.substring(1);
                cvPrefix.add(cvName.trim());
              } else if(line.contains("Exploris")){
                isSplittable = true;
              }
            }
          } //End read InstrumentMethods
        } //End go through User text
      }

      LOG.trace(" is mzDB file USerText 'Exploris'? {} with {} CV", isSplittable, nbrCV);
      if (!isSplittable || nbrCV <=1) {
        LOG.warn(" --- The specified file is not an Exploris result or no CV has been defined");
        if(!isSplittable)
          m_finishCode = RETURN_CODE.NOT_EXPLORIS;
        else if(nbrCV <= 1)
          m_finishCode = RETURN_CODE.NO_CVS;
        m_mzDbReader.close();
        return false;
      } else {

        m_outputMzdbFiles = new ArrayList<>(cvPrefix.size());
        MzDBMetaData mzDbMetaData = MzDBUtil.createMzDbMetaData(m_mzDbReader);
        AcquisitionMode srcAcqMode = m_mzDbReader.getAcquisitionMode();
        boolean isDIA = (srcAcqMode != null && srcAcqMode.equals(fr.profi.mzdb.model.AcquisitionMode.SWATH));

        for (String nextCV : cvPrefix) {
          File f = getOutFile(nextCV);
          m_outputMzdbFiles.add(f);
          MzDBWriter writer = new MzDBWriter(f, false, mzDbMetaData, defaultBBsize, isDIA);
          writer.initialize();
          writerPerCV.put(nextCV, writer);
        }
        LOG.trace(" Writers created for each CVs");

        long start1 = System.currentTimeMillis();
        int rewritedScanCount = 0;
        int readScanCount = 0;

        //VDS for timing debug, to remove
//        long time_read3 = 0;
//        long time_read1 = 0;
//        long time_read2 = 0;
//        long time_read4=0;
//        long time_read5=0;
//        long time_write = 0;
//        long time_readfull = 0;
//        long time_writefull = 0;
        for (SpectrumHeader srcSpectrumHeader : headers) {

          readScanCount++;
          MzDBWriter spectrumWriter = null;
          List<CVParam> params = srcSpectrumHeader.getCVParams();
          if(params != null && ! params.isEmpty()) {
            for (CVParam nextParam : params){
              if(nextParam.getAccession().equals(CV_PARAM_ACC) && cvPrefix.contains(nextParam.getValue())) {
                spectrumWriter = writerPerCV.get(nextParam.getValue()) ;
                break;
              }
            }

            if(spectrumWriter == null) {
              LOG.warn("Spectra {} (tile {}) has NO CV. Will not be write in any output file ", srcSpectrumHeader.getSpectrumId(), srcSpectrumHeader.getTitle());
              continue;
            }

            Spectrum srcSpectrum = m_mzDbReader.getSpectrum(srcSpectrumHeader.getSpectrumId());


//            long start = System.currentTimeMillis();
//            long id = srcSpectrumHeader.getSpectrumId();
//            long step1 = System.currentTimeMillis();
//            time_read1 += step1-start;
//            String paramTreeAsString = srcSpectrumHeader.getParamTreeAsString(m_mzDbReader.getConnection());
//            long step2 = System.currentTimeMillis();
//            time_read2 += step2 - step1;
//            String scanlists =  srcSpectrumHeader.getScanListAsString(m_mzDbReader.getConnection());
//            long step3 = System.currentTimeMillis();
//            time_read3 += step3 - step2;
//            String precStr =  srcSpectrumHeader.getPrecursorListAsString(m_mzDbReader.getConnection());
//            long step4 = System.currentTimeMillis();
//            time_read4 += step4 - step3;

            SpectrumMetaData spectrumMetaData = new SpectrumMetaData(
                    srcSpectrumHeader.getSpectrumId(),
                    srcSpectrumHeader.getParamTreeAsString(m_mzDbReader.getConnection()),
                    srcSpectrumHeader.getScanListAsString(m_mzDbReader.getConnection()),
                    srcSpectrumHeader.getPrecursorListAsString(m_mzDbReader.getConnection()));
//            long end = System.currentTimeMillis();
//            time_read5 += end-step4;
            spectrumWriter.insertSpectrum(srcSpectrum, spectrumMetaData, m_mzDbReader.getSpectrumDataEncoding(srcSpectrumHeader.getSpectrumId()));
//            long endw = System.currentTimeMillis();
//            time_write += endw - end;
            rewritedScanCount++;
            if (readScanCount % 5000 == 0 || srcSpectrumHeader.getSpectrumId() == m_mzDbReader.getSpectraCount()) {
              LOG.info("Read {} spectra over {} ({} already rewrited)", readScanCount, m_mzDbReader.getSpectraCount(), rewritedScanCount);
//              LOG.debug("Time used to read 1 : " + time_read1);
//              LOG.debug("Time used to read 2 : " + time_read2);
//              LOG.debug("Time used to read 3 : " + time_read3);
//              LOG.debug("Time used to read 4 : " + time_read4);
//              LOG.debug("Time used to read 5 : " + time_read5);
//              LOG.debug("Time used to write : " + time_write);
//              time_readfull += start-end;
//              time_writefull += time_write;
//              time_read1 = 0;
//              time_read2 = 0;
//              time_read3 = 0;
//              time_read4 = 0;
//              time_read5 = 0;
//              time_write = 0;
            }
          } else {
              LOG.warn("Spectra {} (tile {})  has NO CV. Will not be write in any output file ", srcSpectrumHeader.getSpectrumId(), srcSpectrumHeader.getTitle());
          }
        }//End go through all spectra

        long end2 = System.currentTimeMillis();
        LOG.info("{} spectrum rewrited  in {} ms", rewritedScanCount, (end2-start1));
//        LOG.debug("Time used to read all : " + time_readfull);
//        LOG.debug("Time used to write all : " + time_writefull);
        writerPerCV.values().forEach(w -> w.close());

//        } //End go through CVs
      }
      m_mzDbReader.close();
      m_finishCode = RETURN_CODE.OK;
      return true;
    } catch(SQLiteException | StreamCorruptedException e){
        e.printStackTrace();
        m_mzDbReader.close();
        m_finishCode =RETURN_CODE.EXCEPTION;
        writerPerCV.values().forEach(w -> w.close());
        return false;
    }

  }
}
