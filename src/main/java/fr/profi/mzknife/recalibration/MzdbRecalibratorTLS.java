package fr.profi.mzknife.recalibration;

import com.almworks.sqlite4java.SQLiteException;
import com.github.mzdb4s.db.model.*;
import com.github.mzdb4s.db.model.params.*;
import com.github.mzdb4s.db.model.params.param.CVParam;
import com.github.mzdb4s.db.model.params.param.UserParam;
import com.github.mzdb4s.io.writer.MzDbWriter;
import com.github.mzdb4s.msdata.*;
import com.github.sqlite4s.SQLiteFactory$;
import fr.profi.mzdb.MzDbReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Enumeration;
import scala.None$;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/*
 * Recalibrate a mzdb file, use mzDbWriter from Toulouse (test to compare writers)
 */
public class MzdbRecalibratorTLS {

  private final static Logger LOG = LoggerFactory.getLogger(MzdbRecalibrator.class);
  private final static scala.Option NoneOption = None$.MODULE$;

  private final MzDbReader m_srcReader;
  private final File m_dstFile;
  DataEncoding m_profileDataEncoding;
  DataEncoding m_centroidDataEncoding;
  DataEncoding m_fittedDataEncoding;


  public MzdbRecalibratorTLS(MzDbReader srcReader, File dstFile)  {
    m_dstFile = dstFile;
    m_srcReader = srcReader;
    m_srcReader.enableParamTreeLoading();
  }

  private MzDbMetaData createMzDbMetaData() throws SQLiteException {

    ParamTree paramTree = new ParamTree();

    List<UserParam> userParams = new ArrayList<>();
    for (fr.profi.mzdb.db.model.params.param.UserParam srcUserParam :  m_srcReader.getMzDbHeader().getUserParams()) {
      userParams.add(new UserParam(srcUserParam.getName(), srcUserParam.getValue(), srcUserParam.getType()));
    }
    paramTree.setUserParams(JavaConverters.asScalaIteratorConverter(userParams.iterator()).asScala().toSeq());
    int currentTime = new Long(System.currentTimeMillis()).intValue();
    MzDbHeader mzdbHeader = new MzDbHeader(m_srcReader.getMzDbHeader().getVersion(), currentTime, paramTree);

    List<DataEncoding> dataEncodings = new ArrayList<>();
    for (int i = 1; i <= m_srcReader.getDataEncodingsCount(); i++) {
      fr.profi.mzdb.model.DataEncoding srcEncoding = m_srcReader.getDataEncoding(i);
      Enumeration.Value dstPeakEncoding = toDataEncoding(srcEncoding);
      switch (srcEncoding.getMode()) {
        case FITTED:
          m_fittedDataEncoding = new DataEncoding(-1, DataMode.FITTED(), dstPeakEncoding, srcEncoding.getCompression(), srcEncoding.getByteOrder());
          dataEncodings.add(m_fittedDataEncoding);
          break;
        case PROFILE:
          m_profileDataEncoding = new DataEncoding(-1, DataMode.PROFILE(), dstPeakEncoding, srcEncoding.getCompression(), srcEncoding.getByteOrder());
          dataEncodings.add(m_profileDataEncoding);
          break;
        case CENTROID:
          m_centroidDataEncoding = new DataEncoding(-1, DataMode.CENTROID(), dstPeakEncoding, srcEncoding.getCompression(), srcEncoding.getByteOrder());
          dataEncodings.add(m_centroidDataEncoding);
      }
    }


    List<InstrumentConfiguration> instrumentConfigurations = new ArrayList<>();

    // creates a Fake instrument

    List<Component> compos = new ArrayList<>();
    SourceComponent srcCompo = new SourceComponent(1);
    //Hard CODED for exemple !
    List<CVParam> params = new ArrayList<>();
    //<cvParam cvRef="MS" accession="MS:1000398" name="nanoelectrospray" value=""/>
    params.add(new CVParam("MS:1000398", "nanoelectrospray", "", "MS", NoneOption, NoneOption, NoneOption));
    //<cvParam cvRef="MS" accession="MS:1000485" name="nanospray inlet" value=""/>
    params.add(new CVParam("MS:1000485", "nanospray inlet", "", "MS", NoneOption, NoneOption, NoneOption));
    srcCompo.setCVParams(JavaConverters.asScalaIteratorConverter(params.iterator()).asScala().toSeq());
    compos.add(srcCompo);
    //VDS TODO  Add Analyzer and Detector component
    ComponentList compList = new ComponentList( JavaConverters.asScalaIteratorConverter(compos.iterator()).asScala().toSeq());

    InstrumentConfiguration instrumentConfiguration = new InstrumentConfiguration(-1, "FakeMS", 1, new ParamTree(), compList);
    instrumentConfigurations.add(instrumentConfiguration);

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


    // Processing methods ... except that there is no api in mzdb-access to read Processing methods.
    List<ProcessingMethod> processingMethods = new ArrayList<>();
    int processingMethodsNumber = 0;
    List<Software> softwares = new ArrayList<>();

    for (fr.profi.mzdb.db.model.Software srcSoftware : m_srcReader.getSoftwareList()) {
      params = new ArrayList<>();
      for (fr.profi.mzdb.db.model.params.param.CVParam srcParam : srcSoftware.getCVParams()) {
        params.add(toCVParam(srcParam));
      }
      paramTree = new ParamTree();
      paramTree.setCVParams(JavaConverters.asScalaIteratorConverter(params.iterator()).asScala().toSeq());
      Software software = new Software((int) srcSoftware.getId(), srcSoftware.getName(), srcSoftware.getVersion(), paramTree);
      softwares.add(software);

      ProcessingMethod pm = new ProcessingMethod((int)srcSoftware.getId(), processingMethodsNumber++, "fake processing method - to be done", scala.Option.apply(new ParamTree()), software.getId());
      processingMethods.add(pm);

    }

    List<Run> runs = new ArrayList<>();
    for( fr.profi.mzdb.db.model.Run srcRun : m_srcReader.getRuns()) {
      Run r = new Run((int)srcRun.getId(), srcRun.getName() , srcRun.getStartTimestamp());
      runs.add(r);
    }

    List<Sample> samples = new ArrayList<>();
    for (fr.profi.mzdb.db.model.Sample srcSample: m_srcReader.getSamples()) {
      paramTree = new ParamTree();
      paramTree.setCVParams(JavaConverters.asScalaIteratorConverter(toCVParams(srcSample).iterator()).asScala().toSeq());
      Sample sample = new Sample((int)srcSample.getId(), srcSample.getName(),paramTree);
      samples.add(sample);
    }


    List<SourceFile> sourceFiles = new ArrayList<>();
    for (fr.profi.mzdb.db.model.SourceFile srcSourceFile: m_srcReader.getSourceFiles()) {
      paramTree = new ParamTree();
      paramTree.setCVParams(JavaConverters.asScalaIteratorConverter(toCVParams(srcSourceFile).iterator()).asScala().toSeq());
      SourceFile sourceFile = new SourceFile((int)srcSourceFile.getId(), srcSourceFile.getName(), srcSourceFile.getLocation(),paramTree);
      sourceFiles.add(sourceFile);

    }

    LOG.info("Created MzDbMetaData.");
    return new MzDbMetaData(
            mzdbHeader,
            JavaConverters.asScalaIteratorConverter(dataEncodings.iterator()).asScala().toSeq(),
            new CommonInstrumentParams(-1,new ParamTree()),
            JavaConverters.asScalaIteratorConverter(instrumentConfigurations.iterator()).asScala().toSeq(),
            JavaConverters.asScalaIteratorConverter(processingMethods.iterator()).asScala().toSeq(),
            JavaConverters.asScalaIteratorConverter(runs.iterator()).asScala().toSeq(),
            JavaConverters.asScalaIteratorConverter(samples.iterator()).asScala().toSeq(),
            JavaConverters.asScalaIteratorConverter(softwares.iterator()).asScala().toSeq(),
            JavaConverters.asScalaIteratorConverter(sourceFiles.iterator()).asScala().toSeq());
  }

  private Enumeration.Value toDataEncoding(fr.profi.mzdb.model.DataEncoding srcEncoding) {
    Enumeration.Value dstPeakEncoding = null;
    switch(srcEncoding.getPeakEncoding()) {
      case HIGH_RES_PEAK:
        dstPeakEncoding = PeakEncoding.HIGH_RES_PEAK();
        break;
      case LOW_RES_PEAK:
        dstPeakEncoding = PeakEncoding.LOW_RES_PEAK();
        break;
      case NO_LOSS_PEAK: dstPeakEncoding = PeakEncoding.NO_LOSS_PEAK();
    }
    return dstPeakEncoding;
  }

  private List<CVParam> toCVParams(fr.profi.mzdb.db.model.AbstractTableModel srcModel) {
    List<CVParam> params = new ArrayList<>();
    for (fr.profi.mzdb.db.model.params.param.CVParam srcParam : srcModel.getCVParams()) {
      params.add(toCVParam(srcParam));
    }
    return params;
  }

  private CVParam toCVParam(fr.profi.mzdb.db.model.params.param.CVParam srcParam) {
    scala.Option opUnitCVRef = (srcParam.getUnitCvRef() == null) ? None$.MODULE$ : new Some<>(srcParam.getUnitCvRef());
    scala.Option opUnitAcc = (srcParam.getUnitAccession() == null) ? None$.MODULE$ : new Some<>(srcParam.getUnitAccession());
    scala.Option opUnitName = (srcParam.getUnitName() == null) ? None$.MODULE$ : new Some<>(srcParam.getUnitName());
    CVParam dstParam = new CVParam(srcParam.getAccession(), srcParam.getName(), srcParam.getValue(), srcParam.getCvRef(), opUnitCVRef, opUnitAcc, opUnitName);

    return dstParam;
  }

  public void recalibrate(Long firstScan, Long lastScan, double deltaMass) {
    MzDbWriter writer = null;

    try {


      DefaultBBSizes$ bbsize = DefaultBBSizes$.MODULE$;
      SQLiteFactory$ sf = SQLiteFactory$.MODULE$;

      MzDbMetaData mzDbMetaData = createMzDbMetaData();
      fr.profi.mzdb.model.AcquisitionMode srcAcqMode = m_srcReader.getAcquisitionMode();
      boolean isDIA = (srcAcqMode != null && srcAcqMode.equals(fr.profi.mzdb.model.AcquisitionMode.SWATH));
      writer = new MzDbWriter(m_dstFile, mzDbMetaData, bbsize.apply(), isDIA, sf);
      writer.open();

      fr.profi.mzdb.model.SpectrumHeader[] headers = m_srcReader.getSpectrumHeaders();
      Arrays.sort(headers, Comparator.comparingLong(fr.profi.mzdb.model.SpectrumHeader::getSpectrumId));
      int recalibratedScanCount = 0;

      for (fr.profi.mzdb.model.SpectrumHeader srcSpectrumHeader: headers) {

        long start = System.currentTimeMillis();

        fr.profi.mzdb.model.Spectrum srcSpectrum = m_srcReader.getSpectrum(srcSpectrumHeader.getSpectrumId());
        fr.profi.mzdb.model.SpectrumData srcSpectrumData = srcSpectrum.getData();

        Precursor precursor = null;
        fr.profi.mzdb.db.model.params.Precursor srcPrecursor = srcSpectrumHeader.getPrecursor();

        if (srcPrecursor != null){
          precursor = new Precursor();
          precursor.spectrumRef_$eq(srcPrecursor.getSpectrumRef());
        }

        SpectrumHeader spH = new SpectrumHeader(
                srcSpectrumHeader.getSpectrumId(),
                (int)srcSpectrumHeader.getId(),
                "title", // TODO : read Spectrum.title from the mzdb file
                srcSpectrumHeader.getCycle(),
                srcSpectrumHeader.getTime(),
                srcSpectrumHeader.getMsLevel(),
                NoneOption,
                srcSpectrumHeader.getPeaksCount(),
                false,
                srcSpectrumHeader.getTIC(),
                srcSpectrumHeader.getBasePeakMz(),
                srcSpectrumHeader.getBasePeakIntensity(),
                new Some(srcSpectrumHeader.getPrecursorMz()),
                new Some(srcSpectrumHeader.getPrecursorCharge()),
                (int)srcSpectrumHeader.getSpectrumId(),
                (ScanList) null,
                precursor,
                NoneOption);

        double[] mzValues = null;
        if ((srcSpectrumHeader.getSpectrumId() >= firstScan) && (srcSpectrumHeader.getSpectrumId() <= lastScan)) {
          recalibratedScanCount++;
          mzValues = RecalibrateUtil.recalibrateMasses(srcSpectrumData.getMzList(), deltaMass);
          LOG.info(" Recalibrate spectrum {] at RT {} ", srcSpectrumHeader.getId(), srcSpectrumHeader.getTime());
        } else {
          mzValues = srcSpectrumData.getMzList();
        }
        WrappedArray<Object> mz = WrappedArray.make(mzValues);
        WrappedArray<Object> intensities = WrappedArray.make(srcSpectrumData.getIntensityList());
        WrappedArray<Object> leftHwhm = WrappedArray.make(srcSpectrumData.getLeftHwhmList());
        WrappedArray<Object> rightHwhm = WrappedArray.make(srcSpectrumData.getRightHwhmList());
        SpectrumData spData = new SpectrumData(mz,intensities, leftHwhm, rightHwhm);

        Spectrum mzdb4sSp = new Spectrum(spH, spData);
        SpectrumXmlMetaData spectrumMetaData = new SpectrumXmlMetaData(
                srcSpectrumHeader.getSpectrumId(),
                srcSpectrumHeader.getParamTreeAsString(m_srcReader.getConnection()),
                srcSpectrumHeader.getScanListAsString(m_srcReader.getConnection()),
                new Some(srcSpectrumHeader.getPrecursorListAsString(m_srcReader.getConnection())),
                scala.Option.empty());

        DataEncoding spectrumEncoding = null;
        switch(m_srcReader.getSpectrumDataEncoding(srcSpectrumHeader.getSpectrumId()).getMode()) {
          case FITTED:
            spectrumEncoding = m_fittedDataEncoding;
            break;
          case CENTROID:
            spectrumEncoding = m_centroidDataEncoding;
            break;
          case PROFILE:
            spectrumEncoding = m_profileDataEncoding;
        }
        writer.insertSpectrum(mzdb4sSp, spectrumMetaData, spectrumEncoding);

        if (srcSpectrumHeader.getSpectrumId() % 1000 == 0 || srcSpectrumHeader.getSpectrumId() == m_srcReader.getSpectraCount()) {
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
      LOG.debug("Finally  " + writer.loggerName() + " close ");
      writer.close();
    }

  }

}
