package fr.profi.mzknife.recalibration;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.BBSizes;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.*;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.io.writer.MzDBWriter;
import fr.profi.mzdb.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Recalibrate a mzdb file
 */
public class MzdbRecalibrator {

  private final static Logger LOG = LoggerFactory.getLogger(MzdbRecalibrator.class);

  private final MzDbReader m_srcReader;
  private final File m_dstFile;
  DataEncoding m_profileDataEncoding;
  DataEncoding m_centroidDataEncoding;
  DataEncoding m_fittedDataEncoding;


  public MzdbRecalibrator(MzDbReader srcReader, File dstFile)  {
    m_dstFile = dstFile;
    m_srcReader = srcReader;
    m_srcReader.enableParamTreeLoading();
  }

  private MzDbMetaData createMzDbMetaData() throws SQLiteException {

    ParamTree paramTree = new ParamTree();

    List<UserParam> userParams = new ArrayList<>();
    for (UserParam srcUserParam :  m_srcReader.getMzDbHeader().getUserParams()) {
      userParams.add(new UserParam(null, null, srcUserParam.getName(), srcUserParam.getValue(), srcUserParam.getType()));
    }
    paramTree.setUserParams(userParams);
    int currentTime = Long.valueOf(System.currentTimeMillis()).intValue();
    MzDbHeader mzdbHeader = new MzDbHeader(m_srcReader.getMzDbHeader().getVersion(), currentTime, paramTree, null);

    List<DataEncoding> dataEncodings = new ArrayList<>(Arrays.asList(m_srcReader.getDataEncodingReader().getDataEncodings()));
    List<InstrumentConfiguration> instrumentConfigurations = m_srcReader.getInstrumentConfigurations();

    //TODO mzDBReader fails to read instrumentConfiguration in existing mzdb files
//    for (fr.profi.mzdb.db.model.InstrumentConfiguration srcConfig : m_srcReader.getInstrumentConfigurations()) {
//      List<Component> components = new ArrayList<>();
//      if (srcConfig.getComponentList() != null) {
//        for (fr.profi.mzdb.db.model.params.Component srcComponent : srcConfig.getComponentList().getComponents()) {
//          List<CVParam> params = new ArrayList<>();
//          for (fr.profi.mzdb.db.model.params.param.CVParam srcParam : srcComponent.getCVParams()) {
//            params.add(toCVParam(srcParam));
//          }
//
//          Component component = null;
//          if (srcComponent instanceof fr.profi.mzdb.db.model.params.SourceComponent) {
//            component = new SourceComponent(srcComponent.getOrder());
//          } else if (srcComponent instanceof fr.profi.mzdb.db.model.params.DetectorComponent) {
//            component = new DetectorComponent(component.getOrder());
//          } else if (srcComponent instanceof fr.profi.mzdb.db.model.params.AnalyzerComponent) {
//            component = new AnalyzerComponent(component.getOrder());
//          }
//          component.setCVParams(JavaConverters.asScalaIteratorConverter(params.iterator()).asScala().toSeq());
//          components.add(component);
//        }
//      }
//
//      ComponentList compList = null;
//      if (!components.isEmpty())
//        compList = new ComponentList(JavaConverters.asScalaIteratorConverter(components.iterator()).asScala().toSeq());
//      InstrumentConfiguration instrumentConfiguration = new InstrumentConfiguration(-1, srcConfig.getName(), srcConfig.getSoftwareId(), new ParamTree(), compList);
//      instrumentConfigurations.add(instrumentConfiguration);
//    }


    // Processing methods ... except that there is no api in mzdb-access to read Processing methods. TODO !!
    List<ProcessingMethod> processingMethods = new ArrayList<>();
    int processingMethodsNumber = 0;
    int processingMethodsid = 1;
    List<Software> softwares =  m_srcReader.getSoftwareList();
    for (Software srcSoftware : softwares) {
      ProcessingMethod pm = new ProcessingMethod(processingMethodsid++, null, processingMethodsNumber++, "fake processing method - to be done", (int) srcSoftware.getId());
      processingMethods.add(pm);
    }

    List<Run> runs = m_srcReader.getRuns();
    List<Sample> samples = m_srcReader.getSamples();
    List<SourceFile> sourceFiles = m_srcReader.getSourceFiles();

    LOG.info("Created MzDbMetaData.");
    MzDbMetaData mzMetaData = new MzDbMetaData();
    mzMetaData.setMzdbHeader(mzdbHeader);
    mzMetaData.setDataEncodings(dataEncodings);
    mzMetaData.setCommonInstrumentParams(new CommonInstrumentParams(-1,new ParamTree()));
    mzMetaData.setInstrumentConfigurations(instrumentConfigurations);
    mzMetaData.setProcessingMethods(processingMethods);
    mzMetaData.setRuns(runs);
    mzMetaData.setSamples(samples);
    mzMetaData.setSourceFiles(sourceFiles);
    mzMetaData.setSoftwares(softwares);
    return mzMetaData;
  }



  public void recalibrate(Long firstScan, Long lastScan, double deltaMass) {
    MzDBWriter writer = null;

    try {

      BBSizes defaultBBsize = new BBSizes(5,10000,15,0);

      MzDbMetaData mzDbMetaData = createMzDbMetaData();
      AcquisitionMode srcAcqMode = m_srcReader.getAcquisitionMode();
      boolean isDIA = (srcAcqMode != null && srcAcqMode.equals(fr.profi.mzdb.model.AcquisitionMode.SWATH));
      writer = new MzDBWriter(m_dstFile, mzDbMetaData, defaultBBsize, isDIA);
      writer.initialize();

      SpectrumHeader[] headers = m_srcReader.getSpectrumHeaders();
      Arrays.sort(headers, Comparator.comparingLong(SpectrumHeader::getSpectrumId));
      int recalibratedScanCount = 0;

      for (SpectrumHeader srcSpectrumHeader: headers) {

//        long start = System.currentTimeMillis();

        Spectrum srcSpectrum = m_srcReader.getSpectrum(srcSpectrumHeader.getSpectrumId());
        SpectrumData srcSpectrumData = srcSpectrum.getData();
        double[] mzValues = null;
        if ((srcSpectrumHeader.getSpectrumId() >= firstScan) && (srcSpectrumHeader.getSpectrumId() <= lastScan)) {
          recalibratedScanCount++;
          mzValues = RecalibrateUtil.recalibrateMasses(srcSpectrumData.getMzList(), deltaMass);
          LOG.info(" Recalibrate spectrum {] at RT {} ", srcSpectrumHeader.getId(), srcSpectrumHeader.getTime());
        } else {
          mzValues = srcSpectrumData.getMzList();
        }
        SpectrumData spData = new SpectrumData(mzValues,srcSpectrumData.getIntensityList(), srcSpectrumData.getLeftHwhmList(), srcSpectrumData.getRightHwhmList());

        Spectrum mzdb4sSp = new Spectrum(srcSpectrumHeader, spData);
        SpectrumMetaData spectrumMetaData = new SpectrumMetaData(
                srcSpectrumHeader.getSpectrumId(),
                srcSpectrumHeader.getParamTreeAsString(m_srcReader.getConnection()),
                srcSpectrumHeader.getScanListAsString(m_srcReader.getConnection()),
                srcSpectrumHeader.getPrecursorListAsString(m_srcReader.getConnection()));

        writer.insertSpectrum(mzdb4sSp, spectrumMetaData, m_srcReader.getSpectrumDataEncoding(srcSpectrumHeader.getSpectrumId()));

        if (srcSpectrumHeader.getSpectrumId() % 5000 == 0 || srcSpectrumHeader.getSpectrumId() == m_srcReader.getSpectraCount()) {
          LOG.info("Write {} spectra over {} ({} already recalibrated)", srcSpectrumHeader.getSpectrumId(), m_srcReader.getSpectraCount(), recalibratedScanCount);
        }
      }//End go through all spectra
      LOG.info("{} spectrum recalibrated", recalibratedScanCount);
    } catch (Exception e) {
      LOG.error("Exception in Spectrum iterator ", e);
      e.printStackTrace();
    } catch (Throwable t) {
      LOG.error("Throwable in Spectrum iterator ", t);
      t.printStackTrace();
    } finally {
      LOG.debug("Finally   writer close ");
      writer.close();
    }

  }

}
