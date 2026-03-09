package fr.profi.mzknife.mzdb;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.BBSizes;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.SharedParamTree;
import fr.profi.mzdb.db.model.params.param.CVEntry;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.db.model.params.param.UserText;
import fr.profi.mzdb.io.util.MzDBUtil;
import fr.profi.mzdb.io.writer.MzDBWriter;
import fr.profi.mzdb.io.writer.ParamTreeStringifier;
import fr.profi.mzdb.model.*;
import fr.profi.mzknife.util.ParamsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StreamCorruptedException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    rootFileName = rootFileName +"_"+ prefix.trim() + m_fileExtension;
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

      final IonMobilityMode ionMobilityMode = m_mzDbReader.getIonMobilityMode();

      if ((ionMobilityMode == null) || (ionMobilityMode.getSeparationValues().size() <=1)) {
        LOG.warn(" --- The specified file is not a FAIMS acquisition or no CV has been defined");
        if(ionMobilityMode == null)
          m_finishCode = RETURN_CODE.NOT_EXPLORIS;
        else
          m_finishCode = RETURN_CODE.NO_CVS;
        m_mzDbReader.close();
        return false;
      } else {

        m_outputMzdbFiles = new ArrayList<>(ionMobilityMode.getSeparationValues().size());
        Map<Long, Long> newIndexByOldIndex = new HashMap<>();
        Map<String, Long> nextIndexByCvPrefix = new HashMap<>();
        MzDBMetaData mzDbMetaData = MzDBUtil.createMzDbMetaData(m_mzDbReader);
        AcquisitionMode srcAcqMode = m_mzDbReader.getAcquisitionMode();
        boolean isDIA = (srcAcqMode != null && srcAcqMode.equals(fr.profi.mzdb.model.AcquisitionMode.SWATH));

        for (String nextCV : ionMobilityMode.getSeparationValues()) {
          File f = getOutFile(nextCV);
          m_outputMzdbFiles.add(f);
          MzDBWriter writer = new MzDBWriter(f, false, mzDbMetaData, defaultBBsize, isDIA);
          writer.initialize();
          writerPerCV.put(nextCV, writer);
          nextIndexByCvPrefix.put(nextCV, 1L);
        }
        LOG.trace(" Writers created for each CVs");

        long start1 = System.currentTimeMillis();
        int rewritedScanCount = 0;
        int readScanCount = 0;

        Pattern spectrumTitlePattern = Pattern.compile("[\\w]*scan[\\s]*=[\\s]*([\\d]+)");

        for (SpectrumHeader srcSpectrumHeader : headers) {

          readScanCount++;
          MzDBWriter spectrumWriter = null;
          long nextIndex = -1L;
          CVParam param = srcSpectrumHeader.getCVParam(CVEntry.FAIMS_COMPENSATION_VOLTAGE);

          if (param != null) {
            String nextVal = String.valueOf(Float.valueOf(param.getValue()).intValue());
            if (ionMobilityMode.getSeparationValues().contains(nextVal)) {
              spectrumWriter = writerPerCV.get(nextVal);
              nextIndex = nextIndexByCvPrefix.get(nextVal);
              nextIndexByCvPrefix.put(nextVal, nextIndex + 1);
            }
          }

            if(spectrumWriter == null) {
              LOG.warn(" !!!!! Spectra {} (tile {}) has NO CV. Will not be write in any output file ", srcSpectrumHeader.getSpectrumId(), srcSpectrumHeader.getTitle());
              continue;
            }

            Spectrum srcSpectrum = m_mzDbReader.getSpectrum(srcSpectrumHeader.getSpectrumId());
            Long initialId = srcSpectrum.getHeader().getId();
            newIndexByOldIndex.put(initialId, nextIndex);

            boolean scanUpdated = false;
            if(srcSpectrum.getHeader().getScanList() != null
                    && srcSpectrum.getHeader().getScanList().getScans() != null
                    && !srcSpectrum.getHeader().getScanList().getScans().isEmpty()) {
              List<UserParam> uParams = srcSpectrum.getHeader().getScanList().getScans().get(0).getUserParams();

              if(uParams != null && !uParams.isEmpty() ) {
                for(int i = 0; i<uParams.size(); i++) {
                  UserParam uParam = uParams.get(i);
                  if (ParamsHelper.UP_MASTER_SCAN_NAME.equals(uParam.getName())) {
                    Integer prevScIndex = Integer.parseInt(uParam.getValue());
                    if(prevScIndex>0) {
                      Long newScIndex = newIndexByOldIndex.get(prevScIndex.longValue());
                      uParam.setValue(newScIndex.toString());
                      uParams.set(i, uParam);
                      scanUpdated = true;
                    }
                    break;
                  }
                }
              }
            }
            String title = srcSpectrum.getHeader().getTitle();
            Matcher titleMatcher = spectrumTitlePattern.matcher(title);
            if(titleMatcher.find() && Long.valueOf(titleMatcher.group(1)).equals(initialId) ){
              title = titleMatcher.replaceAll("scan="+newIndexByOldIndex.get(initialId));
              srcSpectrum.getHeader().setTitle(title);
            }

            String scanAsString = scanUpdated ? ParamTreeStringifier.stringifyScanList(srcSpectrumHeader.getScanList()) : srcSpectrumHeader.getScanListAsString(m_mzDbReader.getConnection());

            SpectrumMetaData spectrumMetaData = new SpectrumMetaData(
                    srcSpectrumHeader.getSpectrumId(),
                    srcSpectrumHeader.getParamTreeAsString(m_mzDbReader.getConnection()),
                    scanAsString,
                    srcSpectrumHeader.getPrecursorListAsString(m_mzDbReader.getConnection()));

            spectrumWriter.insertSpectrum(srcSpectrum, spectrumMetaData, m_mzDbReader.getSpectrumDataEncoding(srcSpectrumHeader.getSpectrumId()));

            rewritedScanCount++;
            if (readScanCount % 5000 == 0 || srcSpectrumHeader.getSpectrumId() == m_mzDbReader.getSpectraCount()) {
              LOG.info("Read {} spectra over {} ({} already rewrited)", readScanCount, m_mzDbReader.getSpectraCount(), rewritedScanCount);

            }

        }//End go through all spectra

        long end2 = System.currentTimeMillis();
        LOG.info("{} spectrum rewrited  in {} ms", rewritedScanCount, (end2-start1));

        writerPerCV.values().forEach(w -> w.close());

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

//  public static Result getResult(MzDbReader reader) throws SQLiteException {
//
//    //To know if mzdb is issue from an exploris !
//    // 1. table shared_param_tree ; col  :data
//    //<referenceableParamGroup id="CommonInstrumentParams">
//    //  <cvParam cvRef="MS" accession="MS:1003028" name="Orbitrap Exploris 480" value=""/>
//    // 2. table mzdb ; col : param tree
//    // <userText cvRef="MS" accession="MS:-1" name="instrumentMethods" type="xsd:string">
//    // ...
//    // Instrument: U3000Nano on exploris-480
//    //...
//    //Global Settings
//    // ...
//    //   FAIMS Gas = 0
//    //   FAIMS Mode = Standard Resolution
//    //...
//    //Experiment 1
//    //   Experiment Name = -50 ...
//    //   FAIMS CV = -50
//    // ..
//    //Experiment 1
//    //   Experiment Name =  -70 ...
//    //   FAIMS CV = -70
//    // ..
//
//
//    List<SharedParamTree> sharedParamTrees = reader.getSharedParamTreeList();
//    boolean isSplittable = false;
//    for (SharedParamTree sharedParamTree : sharedParamTrees) {
//      List<CVParam> params = sharedParamTree.getData().getCVParams();
//      for (CVParam p : params) {
//        if (p.getName().toLowerCase().contains("exploris")) {
//          isSplittable = true;
//          break;
//        }
//      }
//    }
//
//    LOG.trace(" is mzDB file Shared Param Tree 'Exploris'? "+isSplittable);
//
//    List<String> cvPrefix = new ArrayList<>();
//    List<UserText> userParams = reader.getMzDbHeader().getUserTexts();
//    if (userParams != null && !userParams.isEmpty()) {
//      for (UserText userText : userParams) {
//        if (USER_TEXT_TAG.equals(userText.getName())) {
//          String instrumMethods = userText.getText();
//          Scanner scanner = new Scanner(instrumMethods);
//          while (scanner.hasNextLine()) {
//            String line = scanner.nextLine().trim();
//            if (line.startsWith(FAIMS_CV_PREFIX)) { //Got a CV
//              isSplittable = true; // if not found in SharedParamTree
//              String cvName = line.substring(8).trim();
//              if (cvName.startsWith("="))
//                cvName = cvName.substring(1);
//              String cvValue = String.valueOf(Float.valueOf(cvName.trim()).intValue());
//              cvPrefix.add(cvValue);
//            } else if(line.contains("Exploris")){
//              isSplittable = true;
//            }
//          }
//        } //End read InstrumentMethods
//      } //End go through User text
//    }
//    Result result = new Result(isSplittable, cvPrefix);
//    return result;
//  }
//
//  public record Result(boolean isSplittable, List<String> cvValues) {
//  }

}
