package fr.profi.mzdb.io.writer;

//import com.almworks.sqlite4java.SQLiteException;
//import com.github.mzdb4s.db.model.*;
//import com.github.mzdb4s.db.model.params.*;
//import com.github.mzdb4s.db.model.params.param.CVParam;
//import com.github.mzdb4s.db.model.params.param.UserParam;
//import com.github.mzdb4s.io.writer.MzDbWriter;
//import com.github.mzdb4s.msdata.*;
//import com.github.sqlite4s.SQLiteFactory$;
//import fr.profi.mzdb.MzDbReader;
//import org.junit.Assert;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import scala.Enumeration;
//import scala.None$;
//import scala.Some;
//import scala.collection.JavaConverters;
//import scala.collection.mutable.WrappedArray;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.StreamCorruptedException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;

public class MzDbWriterTLSTest {
//
//  final static Logger logger = LoggerFactory.getLogger(MzDbWriterTLSTest.class);
//
//  private static String srcFilename = "/OVEMB150205_12.raw.0.9.8.mzDB";
//  private static String destFilename = "TLS_OVEMB150205_12.raw.0.9.8.mzDB";
////private static String srcFilename = "C:\\vero\\DEV\\Proline\\mzdb\\frag\\QEx2_020038.mzdb";
////private static String destFilename = "C:\\vero\\DEV\\Proline\\mzdb\\frag\\NewQEx2_020038.mzdb";
//
//  private final static scala.Option NoneOption = None$.MODULE$;
//
////  @Test
//  public void testReadWrite() throws SQLiteException {
//    MzDbReader mzDb = null;
//    MzDbWriter mzDbWriter = null;
//    DataEncoding m_profileDataEncoding= null;
//    DataEncoding m_centroidDataEncoding= null;
//    DataEncoding m_fittedDataEncoding = null;
//
//    File fSrc =new File(MzDbWriterTLSTest.class.getResource(srcFilename).getFile());
//    File fDest = new File(fSrc.getParent(),destFilename);
////    File fSrc =new File(srcFilename);
////    File fDest =new File(destFilename);
//    if(fDest.exists())
//      fDest.delete();
//
//    logger.info("Non Regression test reading mzDB file " + srcFilename + ". Write to "+destFilename);
//    try {
//      mzDb = new MzDbReader(fSrc, true);
//      mzDb.enableScanListLoading();
//      mzDb.enableParamTreeLoading();
//      mzDb.enablePrecursorListLoading();
//      Assert.assertNotNull("Reader cannot be created", mzDb);
//    } catch (ClassNotFoundException | FileNotFoundException | SQLiteException e) {
//      Assert.fail("MzDB reader instantiation exception " + e.getMessage() + " for " + srcFilename);
//    }
//
//    ParamTree paramTree = new ParamTree();
//
//    List<UserParam> userParams = new ArrayList<>();
//    for (fr.profi.mzdb.db.model.params.param.UserParam srcUserParam :  mzDb.getMzDbHeader().getUserParams()) {
//      userParams.add(new UserParam(srcUserParam.getName(), srcUserParam.getValue(), srcUserParam.getType()));
//    }
//    paramTree.setUserParams(JavaConverters.asScalaIteratorConverter(userParams.iterator()).asScala().toSeq());
//    int currentTime = new Long(System.currentTimeMillis()).intValue();
//    MzDbHeader mzdbHeader = new MzDbHeader(mzDb.getMzDbHeader().getVersion(), currentTime, paramTree);
//
//    List<DataEncoding> dataEncodings = new ArrayList<>();
//    fr.profi.mzdb.model.DataEncoding[] srcDE = mzDb.getDataEncodingReader().getDataEncodings();
//    for (fr.profi.mzdb.model.DataEncoding nextDE :  srcDE) {
//      Enumeration.Value dstPeakEncoding = toDataEncoding(nextDE);
//      switch (nextDE.getMode()) {
//        case FITTED:
//          m_fittedDataEncoding = new DataEncoding(-1, DataMode.FITTED(), dstPeakEncoding, nextDE.getCompression(), nextDE.getByteOrder());
//          dataEncodings.add(m_fittedDataEncoding);
//          break;
//        case PROFILE:
//          m_profileDataEncoding = new DataEncoding(-1, DataMode.PROFILE(), dstPeakEncoding, nextDE.getCompression(), nextDE.getByteOrder());
//          dataEncodings.add(m_profileDataEncoding);
//          break;
//        case CENTROID:
//          m_centroidDataEncoding = new DataEncoding(-1, DataMode.CENTROID(), dstPeakEncoding, nextDE.getCompression(), nextDE.getByteOrder());
//          dataEncodings.add(m_centroidDataEncoding);
//      }
//    }
//
//    List<InstrumentConfiguration> instrumentConfigurations = new ArrayList<>();
//      for (fr.profi.mzdb.db.model.InstrumentConfiguration srcConfig : mzDb.getInstrumentConfigurations()) {
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
//            component = new DetectorComponent(srcComponent.getOrder());
//          } else if (srcComponent instanceof fr.profi.mzdb.db.model.params.AnalyzerComponent) {
//            component = new AnalyzerComponent(srcComponent.getOrder());
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
//
//    List<ProcessingMethod> processingMethods = new ArrayList<>();
//    int processingMethodsNumber = 0;
//    List<Software> softwares = new ArrayList<>();
//    List<CVParam> params;
//    for (fr.profi.mzdb.db.model.Software srcSoftware : mzDb.getSoftwareList()) {
//      params = new ArrayList<>();
//      for (fr.profi.mzdb.db.model.params.param.CVParam srcParam : srcSoftware.getCVParams()) {
//        params.add(toCVParam(srcParam));
//      }
//      paramTree = new ParamTree();
//      paramTree.setCVParams(JavaConverters.asScalaIteratorConverter(params.iterator()).asScala().toSeq());
//      Software software = new Software((int) srcSoftware.getId(), srcSoftware.getName(), srcSoftware.getVersion(), paramTree);
//      softwares.add(software);
//
//      ProcessingMethod pm = new ProcessingMethod((int)srcSoftware.getId(), processingMethodsNumber++, "fake processing method - to be done", scala.Option.apply(new ParamTree()), software.getId());
//      processingMethods.add(pm);
//
//    }
//
//    List<Run> runs = new ArrayList<>();
//    for( fr.profi.mzdb.db.model.Run srcRun : mzDb.getRuns()) {
//      Run r = new Run((int)srcRun.getId(), srcRun.getName() , srcRun.getStartTimestamp());
//      runs.add(r);
//    }
//    List<Sample> samples = new ArrayList<>();
//    for (fr.profi.mzdb.db.model.Sample srcSample: mzDb.getSamples()) {
//      paramTree = new ParamTree();
//      paramTree.setCVParams(JavaConverters.asScalaIteratorConverter(toCVParams(srcSample).iterator()).asScala().toSeq());
//      Sample sample = new Sample((int)srcSample.getId(), srcSample.getName(),paramTree);
//      samples.add(sample);
//    }
//
//    List<SourceFile> sourceFiles = new ArrayList<>();
//    for (fr.profi.mzdb.db.model.SourceFile srcSourceFile: mzDb.getSourceFiles()) {
//      paramTree = new ParamTree();
//      paramTree.setCVParams(JavaConverters.asScalaIteratorConverter(toCVParams(srcSourceFile).iterator()).asScala().toSeq());
//      SourceFile sourceFile = new SourceFile((int)srcSourceFile.getId(), srcSourceFile.getName(), srcSourceFile.getLocation(),paramTree);
//      sourceFiles.add(sourceFile);
//
//    }
//
//    logger.info("Created MzDBMetaData.");
//    MzDbMetaData metaData =  new MzDbMetaData(
//            mzdbHeader,
//            JavaConverters.asScalaIteratorConverter(dataEncodings.iterator()).asScala().toSeq(),
//            new CommonInstrumentParams(-1,new ParamTree()),
//            JavaConverters.asScalaIteratorConverter(instrumentConfigurations.iterator()).asScala().toSeq(),
//            JavaConverters.asScalaIteratorConverter(processingMethods.iterator()).asScala().toSeq(),
//            JavaConverters.asScalaIteratorConverter(runs.iterator()).asScala().toSeq(),
//            JavaConverters.asScalaIteratorConverter(samples.iterator()).asScala().toSeq(),
//            JavaConverters.asScalaIteratorConverter(softwares.iterator()).asScala().toSeq(),
//            JavaConverters.asScalaIteratorConverter(sourceFiles.iterator()).asScala().toSeq());
//
//    try {
//
//      DefaultBBSizes$ bbsize = DefaultBBSizes$.MODULE$;
//      SQLiteFactory$ sf = SQLiteFactory$.MODULE$;
//
//      mzDbWriter = new MzDbWriter(fDest, metaData, bbsize.apply(), false, sf);
//      mzDbWriter.open();
//
//      int c = mzDb.getSpectraCount();
//      Map<Long, fr.profi.mzdb.model.DataEncoding> deBySpId = mzDb.getDataEncodingBySpectrumId();
//      logger.info("Writing "+c+" spectrum ");
//      for(int i=1 ; i<=c;i++) {
//        fr.profi.mzdb.model.Spectrum srcSpectrum = mzDb.getSpectrum(i);
//        fr.profi.mzdb.model.SpectrumHeader srcSpectrumHeader =  mzDb.getSpectrumHeader(i);
//        fr.profi.mzdb.model.SpectrumData srcSpectrumData =  srcSpectrum.getData();
//        Precursor precursor = null;
//        fr.profi.mzdb.db.model.params.Precursor srcPrecursor = srcSpectrumHeader.getPrecursor();
//
//        if (srcPrecursor != null){
//          precursor = new Precursor();
//          precursor.spectrumRef_$eq(srcPrecursor.getSpectrumRef());
//        }
//        SpectrumHeader spH = new SpectrumHeader(
//                srcSpectrumHeader.getSpectrumId(),
//                (int)srcSpectrumHeader.getId(),
//                "title", // TODO : read Spectrum.title from the mzdb file
//                srcSpectrumHeader.getCycle(),
//                srcSpectrumHeader.getTime(),
//                srcSpectrumHeader.getMsLevel(),
//                NoneOption,
//                srcSpectrumHeader.getPeaksCount(),
//                false,
//                srcSpectrumHeader.getTIC(),
//                srcSpectrumHeader.getBasePeakMz(),
//                srcSpectrumHeader.getBasePeakIntensity(),
//                new Some(srcSpectrumHeader.getPrecursorMz()),
//                new Some(srcSpectrumHeader.getPrecursorCharge()),
//                (int)srcSpectrumHeader.getSpectrumId(),
//                (ScanList) null,
//                precursor,
//                NoneOption);
//
//        WrappedArray<Object> mz = WrappedArray.make(srcSpectrumData.getMzList());
//        WrappedArray<Object> intensities = WrappedArray.make(srcSpectrumData.getIntensityList());
//        WrappedArray<Object> leftHwhm = WrappedArray.make(srcSpectrumData.getLeftHwhmList());
//        WrappedArray<Object> rightHwhm = WrappedArray.make(srcSpectrumData.getRightHwhmList());
//        SpectrumData spData = new SpectrumData(mz,intensities, leftHwhm, rightHwhm);
//        Spectrum mzdb4sSp = new Spectrum(spH, spData);
//        SpectrumXmlMetaData spectrumMetaData = new SpectrumXmlMetaData(
//                srcSpectrumHeader.getSpectrumId(),
//                srcSpectrumHeader.getParamTreeAsString(mzDb.getConnection()),
//                srcSpectrumHeader.getScanListAsString(mzDb.getConnection()),
//                new Some(srcSpectrumHeader.getPrecursorListAsString(mzDb.getConnection())),
//                scala.Option.empty());
//
//        DataEncoding spectrumEncoding = null;
//        switch(deBySpId.get(srcSpectrumHeader.getSpectrumId()).getMode()) {
//          case FITTED:
//            spectrumEncoding = m_fittedDataEncoding;
//            break;
//          case CENTROID:
//            spectrumEncoding = m_centroidDataEncoding;
//            break;
//          case PROFILE:
//            spectrumEncoding = m_profileDataEncoding;
//        }
//        mzDbWriter.insertSpectrum(mzdb4sSp, spectrumMetaData, spectrumEncoding);
//        if (i % 5000 == 0 || i == c) {
//          logger.info("Already written {} spectra over {} ", i, c);
//        }
//      }
//      logger.info("END Writing  file "+destFilename);
//    } catch (SQLiteException | StreamCorruptedException e) {
//      e.printStackTrace();
//    } finally {
//      mzDb.close();
//      if(mzDbWriter != null)
//        mzDbWriter.close();
//    }
//
//  }
//
//  private Enumeration.Value toDataEncoding(fr.profi.mzdb.model.DataEncoding srcEncoding) {
//    Enumeration.Value dstPeakEncoding = null;
//    switch(srcEncoding.getPeakEncoding()) {
//      case HIGH_RES_PEAK:
//        dstPeakEncoding = PeakEncoding.HIGH_RES_PEAK();
//        break;
//      case LOW_RES_PEAK:
//        dstPeakEncoding = PeakEncoding.LOW_RES_PEAK();
//        break;
//      case NO_LOSS_PEAK: dstPeakEncoding = PeakEncoding.NO_LOSS_PEAK();
//    }
//    return dstPeakEncoding;
//  }
//  private List<CVParam> toCVParams(fr.profi.mzdb.db.model.AbstractTableModel srcModel) {
//    List<CVParam> params = new ArrayList<>();
//    for (fr.profi.mzdb.db.model.params.param.CVParam srcParam : srcModel.getCVParams()) {
//      params.add(toCVParam(srcParam));
//    }
//    return params;
//  }
//
//  private CVParam toCVParam(fr.profi.mzdb.db.model.params.param.CVParam srcParam) {
//    scala.Option opUnitCVRef = (srcParam.getUnitCvRef() == null) ? None$.MODULE$ : new Some(srcParam.getUnitCvRef());
//    scala.Option opUnitAcc = (srcParam.getUnitAccession() == null) ? None$.MODULE$ : new Some<>(srcParam.getUnitAccession());
//    scala.Option opUnitName = (srcParam.getUnitName() == null) ? None$.MODULE$ : new Some<>(srcParam.getUnitName());
//    CVParam dstParam = new CVParam(srcParam.getAccession(), srcParam.getName(), srcParam.getValue(), srcParam.getCvRef(), opUnitCVRef, opUnitAcc, opUnitName);
//
//    return dstParam;
//  }

}
