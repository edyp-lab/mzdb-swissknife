package fr.profi.timstof;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import fr.profi.brucker.timstof.io.TimstofReader;
import fr.profi.brucker.timstof.model.AbstractTimsFrame;
import fr.profi.brucker.timstof.model.SpectrumGeneratingMethod;
import fr.profi.brucker.timstof.model.TimsPASEFFrame;
import fr.profi.mzdb.BBSizes;
import fr.profi.mzdb.db.model.*;
import fr.profi.mzdb.db.model.params.*;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.io.writer.MzDBWriter;
import fr.profi.mzdb.model.*;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Timstof2Mzdb {

    public static final float RT_EPSILON = 0.005f;
    private final static Logger LOG = LoggerFactory.getLogger(Timstof2Mzdb.class);
    static String filepath =  "C:\\Local\\bruley\\Data\\TimsTOF\\2022002-1_eColi-10ng-30min_Slot1-10_1_313.d";
//    static String filepath =  "C:\\vero\\DEV\\TimsTof\\example_data\\Ecoli10ng_2715\\Ecoli10ng60minLCGrad_nanoElute_Slot1-46_01_2715.d";


    private File m_ttFile;
    private SpectrumGeneratingMethod m_ms1Method;
    private long m_fileHdl;
    private TimstofReader m_ttReader;

    //For all Spectra Index, map to associated Frame Index
    private Int2IntMap m_spectra2FrameIndex;
    //Memorize first Spectra Index for each Frame (index)
    private Int2IntMap m_frame2FirstSpectraIndex;
    private Int2IntMap m_frame2ReadSpectraCount;

    private Int2ObjectMap<AbstractTimsFrame> m_frameById;
    private Int2ObjectMap<fr.profi.brucker.timstof.model.Precursor> m_precursorByIds;
    DataEncoding m_profileDataEncoding;
    DataEncoding m_centroidDataEncoding;
    DataEncoding m_fittedDataEncoding;

    public Timstof2Mzdb(File ttFile, SpectrumGeneratingMethod ms1Method){
        this.m_ttFile = ttFile;
        this.m_ms1Method = ms1Method;
        this.m_ttReader = TimstofReader.getTimstofReader();
        this.m_fileHdl = m_ttReader.openTimstofFile(m_ttFile);
        initFramesData();
        m_profileDataEncoding = new DataEncoding(-1, DataMode.PROFILE, PeakEncoding.HIGH_RES_PEAK, "none", ByteOrder.LITTLE_ENDIAN);
        m_centroidDataEncoding = new DataEncoding(-1, DataMode.CENTROID, PeakEncoding.HIGH_RES_PEAK, "none", ByteOrder.LITTLE_ENDIAN);
        m_fittedDataEncoding = new DataEncoding(-1, DataMode.FITTED, PeakEncoding.HIGH_RES_PEAK, "none", ByteOrder.LITTLE_ENDIAN);
    }

    private void closeFile(){
        m_ttReader.closeTimstofFile(m_fileHdl);
    }

    private void initFramesData(){
        long start = System.currentTimeMillis();
        //Read TimsFrames with associated MetaData
        List<AbstractTimsFrame> frames = m_ttReader.getFullTimsFrames(m_fileHdl);
        Collections.sort(frames);

        //Init indexes map
        int spectrumIndex =1;
        m_spectra2FrameIndex = new Int2IntOpenHashMap();
        m_frame2FirstSpectraIndex = new Int2IntOpenHashMap(frames.size()*4/3+1);
        m_frameById = new Int2ObjectOpenHashMap<>(frames.size()*4/3+1);
        for(AbstractTimsFrame tf : frames){
            int nbrSpectrum = tf.getSpectrumCount(); //VDS TODO For now 1 spectrum for MS ==> May have one per scans groups!!

            m_frame2FirstSpectraIndex.put(tf.getId(),spectrumIndex);
            for(int i=0;i<nbrSpectrum; i++){
                m_spectra2FrameIndex.put(spectrumIndex, tf.getId());
                spectrumIndex++;
            }
            m_frameById.put(tf.getId(), tf);
        }
        m_frame2ReadSpectraCount = new Int2IntOpenHashMap(frames.size()*4/3+1);
        m_precursorByIds = m_ttReader.getPrecursorInfoById(m_fileHdl);

        long end  = System.currentTimeMillis();
        LOG.info("Read meta data for "+ frames.size()+ " frames and "+m_spectra2FrameIndex.size()+" spectrum. Duration : "+ (end-start) +" ms");
    }

    private MzDBMetaData createMzDbMetaData(){

        ParamTree pt = new ParamTree();
        List<UserParam> ups = new ArrayList<>();
        ups.add(new UserParam("", "", "origin_file_format", "Timstof Brucker","xsd:string"));
        pt.setUserParams(ups);
        int currentTime = Long.valueOf(System.currentTimeMillis()).intValue();
        MzDbHeader mzdbHeader = new MzDbHeader("0.7", currentTime,  pt, null);

        List<DataEncoding> des = new ArrayList<>();
        des.add(m_profileDataEncoding);
        des.add(m_centroidDataEncoding);
        des.add(m_fittedDataEncoding);

        //Read GlocalPropertie
        Map<String, String> globalProperties = m_ttReader.readGlobalProperties(m_fileHdl);

        //CommonInstrumentParams ex
        /* <referenceableParamGroup id="CommonInstrumentParams">
      <cvParam cvRef="MS" accession="MS:1001911" name="Q Exactive" value=""/>
      <cvParam cvRef="MS" accession="MS:1000529" name="instrument serial number" value="Exactive Series slot #2533"/>
    </referenceableParamGroup> */

        CommonInstrumentParams ciParams  = new CommonInstrumentParams(-1,new ParamTree());
        List<Component> compos = new ArrayList<>();
        SourceComponent srcCompo = new SourceComponent();
        //Hard CODED for exemple !
        List<CVParam> params = new ArrayList<>();
        //<cvParam cvRef="MS" accession="MS:1000398" name="nanoelectrospray" value=""/>
        params.add(buildCVParam("MS:1000398", "nanoelectrospray", "", "MS"));
        //<cvParam cvRef="MS" accession="MS:1000485" name="nanospray inlet" value=""/>
        params.add(buildCVParam("MS:1000485", "nanospray inlet", "", "MS"));
        srcCompo.setCvParams(params);
        compos.add(srcCompo);
        //VDS TODO  Add Analyzer and Detector component
        ComponentList compList = new ComponentList();
        compList.setComponents(compos);

        InstrumentConfiguration iconf = new InstrumentConfiguration(-1,"test",1 , new ParamTree(),compList);
        List<InstrumentConfiguration> iConfList = new ArrayList<>();
        iConfList.add(iconf);

        int indexSoft = 1;
        List<Software> softList = new ArrayList<>();
        ParamTree processMethodParamTree = new ParamTree();
        List<ProcessingMethod> pmList = new ArrayList<>();
        if(globalProperties.containsKey("AcquisitionSoftware")){
            String name = globalProperties.get("AcquisitionSoftware");
            String version = globalProperties.getOrDefault("AcquisitionSoftwareVersion", "Unknown");
            Software soft = new Software(indexSoft++,name,version, new ParamTree());
            softList.add(soft);
        }
        ProcessingMethod pm = new ProcessingMethod(1,processMethodParamTree, 0,"TimsTof_2_mzdb_conversion" , indexSoft);
        pmList.add(pm);
        softList.add( new Software(indexSoft++,"ttofConverter","0.1", new ParamTree()));

        pm = new ProcessingMethod(2,processMethodParamTree, 1,"TimsTof_2_mzdb_conversion" , indexSoft);
        pmList.add(pm);
        softList.add( new Software(indexSoft,"mzDB","0.9.10", new ParamTree()));

        String acqDateStr = globalProperties.get("AcquisitionDateTime");
        Date acqDate =  Calendar.getInstance().getTime();
        if(acqDateStr!=null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            try {
                acqDate = sdf.parse(acqDateStr);
            } catch (ParseException e) {
                acqDate = Calendar.getInstance().getTime();
            }
        }

        String fileName =m_ttFile.getName();
        int pos = fileName.lastIndexOf(".");
        if (pos > 0 && pos < (fileName.length() - 1)) { // If '.' is not the first or last character.
            fileName = fileName.substring(0, pos);
        }

        Run r= new Run(-1,fileName, acqDate);
        List<Run> rList = new ArrayList<>();
        rList.add(r);

        String splName = globalProperties.getOrDefault("SampleName", m_ttFile.getName()+"_Sample");
        Sample s = new Sample(-1,splName, new ParamTree());
        List<Sample> sList = new ArrayList<>();
        sList.add(s);

        SourceFile sFile  = new SourceFile(1,m_ttFile.getName(),m_ttFile.getAbsolutePath(), new ParamTree());
        List<SourceFile> sFileList = new ArrayList<>();
        sFileList.add(sFile);

        LOG.info("Created MzDbMetaData.");
        final MzDBMetaData metaData = new MzDBMetaData();
        metaData.setMzdbHeader(mzdbHeader);
        metaData.setDataEncodings(des);
        metaData.setInstrumentConfigurations(iConfList);
        metaData.setProcessingMethods(pmList);
        metaData.setRuns(rList);
        metaData.setSamples(sList);
        metaData.setSoftwares(softList);
        metaData.setSourceFiles(sFileList);
//TODO      // metaData.xxxx = ciParams;

        final Map<String, String> properties = m_ttReader.readGlobalProperties(m_fileHdl);
        final long mzAcqRangeLower = Math.round(Double.parseDouble(properties.get("MzAcqRangeLower")));
        metaData.setLowestMS1Mz(mzAcqRangeLower);

        return metaData;
    }

    private CVParam buildCVParam(String accession, String name, String value, String cvRef) {
        CVParam param = new CVParam();
        param.setAccession(accession);
        param.setName(name);
        param.setValue(value);
        param.setCvRef(cvRef);

        return param;
    }

    private void createMZdBData(){
        MzDBWriter writer = null;

        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH_mm_ss");
            String date = dateFormat.format(Calendar.getInstance().getTime());
            String outFilePath = m_ttFile.getAbsolutePath() +"_"+date+".mzdb";
            File outFile = new File( outFilePath);
//            BBSizes newNBbSize = new BBSizes(10000, 10000,0,0);
            BBSizes defaultBBsize = new BBSizes(5, 10000, 15, 0);

            MzDBMetaData mzDbMetaData =  createMzDbMetaData();
            writer = new MzDBWriter(outFile, mzDbMetaData, /*newNBbSize*/ defaultBBsize, false);
            writer.initialize();

            int spId = 1; //Spectrum Index start at 1
            int mzDBSpId = spId;
            int cycle =0; //VDS : TODO 1 cycle = 1Ms + xMSMS ?
            int nbrSp  = m_spectra2FrameIndex.size();

            //--> VDS-TIME: to get duration debug info
            long time_readFrame = 0;
            long time_ms2 = 0;
//            long time_fillPrec = 0;
            long time_ms1 = 0;
            long time_write = 0;

            // For test only: allows to test / debug the process of a single scan
            //testFrameAndQuit(177555);

            while(spId <= nbrSp) { //Assume spectrum id start at 1 and are incremental => done in initFrameData above
                long start = System.currentTimeMillis();  //--> VDS-TIME: to get duration debug info

                int frameId = m_spectra2FrameIndex.get(spId);

                //increment nbr spectrum read for specific frameId
                int nbrScanForFrame  = m_frame2ReadSpectraCount.getOrDefault(frameId,0);
                nbrScanForFrame++;
                m_frame2ReadSpectraCount.put(frameId,nbrScanForFrame);

                //get TimsFrame with specific id
                AbstractTimsFrame timsFrame = m_frameById.get(frameId);
                if (timsFrame == null ) {
                    LOG.warn("#### NO FRAME " + frameId+" for spectra "+spId);
                    spId++;
                    continue;
                }
                if (!timsFrame.spectrumRead()) {
                    //get spectrum information if not read yet
                    List<AbstractTimsFrame> tfs = Collections.singletonList(timsFrame);
                    m_ttReader.fillFramesWithSpectrumData(m_fileHdl, tfs);
                }

                Precursor mzdbPrecursor = null;
                fr.profi.brucker.timstof.model.Spectrum ttSpectrum = null;
                Double preMz = null;
                Integer preCharge = null;

                //--> VDS-TIME: to get duration debug info
                long step1 = System.currentTimeMillis();
                time_readFrame += step1-start; //--> VDS-TIME: Ecoli (10Go) ~30min
                long step2;

                float rtInSec = (float) timsFrame.getTime();
                int mslevel = 0;
                boolean errFound = false;
                switch (timsFrame.getMsmsType()) {
                    case MS:
                        // Ms Frame
                        mslevel = 1;
                        cycle++;
                        //Read 'single' spectrum ==> TODO change to read all Scans Spectrum or use groups ??
                        ttSpectrum = timsFrame.getSingleSpectrum(m_ms1Method);

                        //--> VDS-TIME: to get duration debug info
                        step2 = System.currentTimeMillis();
                        time_ms1 += step2 - step1;  //--> VDS-TIME: Ecoli (10Go) ~10min
                        break;

                    case PASEF:
                        mslevel = 2;
                        if( ((TimsPASEFFrame)timsFrame).getPrecursorIds() != null){
                            List<Integer> precursorIds = ((TimsPASEFFrame)timsFrame).getPrecursorIds();
                            Collections.sort(precursorIds);
                            //-- Read spectrum corresponding to index...
                            int indexInFrameSpectra = spId - m_frame2FirstSpectraIndex.get(timsFrame.getId()); //Index relative to frame
                            if (indexInFrameSpectra >= precursorIds.size() || indexInFrameSpectra <0) {
                                LOG.warn("#### WARNING #### can't found precursor id  for spectra " + spId+" Index in frame : "+indexInFrameSpectra+". TimsFrame index"+timsFrame.getId());
                                errFound = true;
                            } else {

                                rtInSec = indexInFrameSpectra == 0 ? rtInSec : rtInSec+(indexInFrameSpectra* RT_EPSILON);

                                //Get spectrum relative to index and specific to precursor
                                int precursorId = precursorIds.get(indexInFrameSpectra);
                                ttSpectrum = ((TimsPASEFFrame)timsFrame).getPrecursorSpectrum(precursorId);
                                if (ttSpectrum == null) {
                                    LOG.warn("#### WARNING #### No precursor spectrum was defined for frame  " + timsFrame.getId() + "; precursorId : " + precursorId + ". Spectrum index " + spId);
                                    errFound = true;
                                } else {

                                    //Create mzDB Precursor using timstof Precursor as model
//                                    long step21 = System.currentTimeMillis();
//                                    time_fillPrec += step21-step1;
                                    fr.profi.brucker.timstof.model.Precursor timstofPrecursor = m_precursorByIds.get(precursorId);
                                    mzdbPrecursor = new Precursor();
                                    mzdbPrecursor.setSpectrumRef(ttSpectrum.getTitle());


                                    if (timstofPrecursor!=null) {
                                        //VDS: Not really sure this is necessary !
//                                        fillmzDBPrecursor(mzdbPrecursor, timstofPrecursor, String.valueOf(timsFrame.getPrecursorCollisionEnergy(precursorId)));
                                        preMz = timstofPrecursor.getMonoIsotopicMz();
                                        preCharge = timstofPrecursor.getCharge();
                                    }
                                 }
//                                //--> VDS-TIME: to get duration debug info
                                step2 = System.currentTimeMillis();
                                time_ms2 += step2 - step1; //--> VDS-TIME: Ecoli (10Go) ~1.5s
                            }
                        } else {
                            LOG.warn("#### WARNING ####  UNSUPPORTED Frame type. Only MS1 and PASEF are supported actually. Frame "+timsFrame.getId()+" is getMsmsType "+timsFrame.getMsmsType());
                            errFound = true;
                        }
                        break;
                    case DIA:
                    case MSMS:
                    default:
                        LOG.warn("#### WARNING ####  UNSUPPORTED Frame type. Only MS1 and PASEF are supported actually. Frame "+timsFrame.getId()+" is getMsmsType "+timsFrame.getMsmsType());
                        errFound = true;
                        break;
                }

                if(errFound){
                    //All spectrum of current frame has been written. Can remove data for memory purpose
                    if(timsFrame.getSpectrumCount() == nbrScanForFrame){
                        timsFrame.clearSpectraData();
                    }
                    spId++;
                    continue;
                }
                step2 = System.currentTimeMillis();//In case wasn't set...should not occur ?!

                fr.profi.mzdb.model.Spectrum mzdbSp = buildMzdbSpectrum(ttSpectrum, mzDBSpId, cycle, rtInSec, mslevel, timsFrame.getSummedIntensity(), preMz, preCharge, mzdbPrecursor);

                if (mzdbSp != null) {
                    SpectrumMetaData spectrumMetaData = new SpectrumMetaData((long) mzDBSpId, "", "", "");

                    DataEncoding spectrumEncoding = m_centroidDataEncoding;
                    if (timsFrame.getMsmsType().equals(AbstractTimsFrame.MsMsType.MS)) {
                        switch (m_ms1Method) {
                            case FULL:
                            case MERGED:
                                spectrumEncoding = m_profileDataEncoding;
                        }
                    }
                    writer.insertSpectrum(mzdbSp, spectrumMetaData, spectrumEncoding);
//                    //VDS: times to get duration debug info
                    long step4 = System.currentTimeMillis();
                    time_write += step4 - step2;//--> VDS-TIME: Ecoli (10Go) ~4.5min
                }

                //All spectrum of current frame has been written. Can remove data for memory purpose
                if (timsFrame.getSpectrumCount() == nbrScanForFrame) {
                    timsFrame.clearSpectraData();
                }

                spId++;
                mzDBSpId++;

                //--> VDS-TIME: to get duration debug info
                if (spId % 1000 == 0 || spId == nbrSp) {
                    LOG.info("Already written {} ({} in mzdb) spectra over {} ", spId, mzDBSpId, nbrSp);
                    LOG.debug("Time used to read Frame: " + time_readFrame);
                    LOG.debug("Time used to create MS2: " + time_ms2);
//                    LOG.info("Time used to create FILLPred: " + time_fillPrec);
                    LOG.debug("Time used to create MS1: " + time_ms1);
                    LOG.debug("Time used to write data: " + time_write);
                    time_readFrame = 0;
                    time_ms2 = 0;
                    time_ms1 = 0;
//                    time_fillPrec = 0;
                    time_write = 0;
                }
            }//End go through all spectra
         } catch(Exception e){
            LOG.error("Exception in Spectrum iterator ",e);
            e.printStackTrace();
        } catch(Throwable t){
            LOG.error("Throwable in Spectrum iterator ",t);
            t.printStackTrace();
        }
        finally {
            LOG.debug("Finally  close writer.");
            writer.close();
        }

    }

    private fr.profi.mzdb.model.Spectrum buildMzdbSpectrum(fr.profi.brucker.timstof.model.Spectrum ttSpectrum, int mzDBSpId, int cycle, float rtInSec, int mslevel, float tic, Double precMz, Integer precCharge, Precursor precursor) {
        float[] intensities = ttSpectrum.getIntensities();
        int nbPeaks = intensities.length;

        if (nbPeaks > 0) { //At least one peak ... VDS TODO:  or create an empty spectrum ?
            int maxIndex = 0;
            float prevIntensity = intensities[0];
            for (int index = 1; index < nbPeaks; index++) {
                float intAtIndex = intensities[index];
                if (intAtIndex > prevIntensity) {
                    maxIndex = index;
                    prevIntensity = intAtIndex;
                }
            }

            SpectrumHeader spH = new SpectrumHeader((long) mzDBSpId, mzDBSpId, cycle, rtInSec, mslevel, ttSpectrum.getTitle(), nbPeaks, false, tic, ttSpectrum.getMasses()[maxIndex], intensities[maxIndex], precMz, precCharge, mzDBSpId, null);
            spH.setPrecursor(precursor);
            SpectrumData spData = new SpectrumData(ttSpectrum.getMasses(), ttSpectrum.getIntensities());
            fr.profi.mzdb.model.Spectrum mzdbSp = new fr.profi.mzdb.model.Spectrum(spH, spData);

            return mzdbSp;

        } else {
            LOG.info("mzdb scan id {} has no peaks ! It will not be written in the mzdb outputfile", mzDBSpId);
            return null;
        }
    }

    private void testFrameAndQuit(int spId) {

        int frameId = m_spectra2FrameIndex.get(spId);
        int nbrScanForFrame  = m_frame2ReadSpectraCount.getOrDefault(frameId,0);
        nbrScanForFrame++;

        //get TimsFrame with specific id
        AbstractTimsFrame timsFrame = m_frameById.get(frameId);
        if (timsFrame != null ) {
            if (!timsFrame.spectrumRead()) {
                //get spectrum information if not read yet
                List<AbstractTimsFrame> tfs = Collections.singletonList(timsFrame);
                m_ttReader.fillFramesWithSpectrumData(m_fileHdl, tfs);
            }
        } else {
            LOG.warn("#### NO FRAME " + frameId+" for spectra "+spId);
        }

        fr.profi.brucker.timstof.model.Spectrum ttSpectrum = timsFrame.getSingleSpectrum(m_ms1Method);
        LOG.info("spectrum {}  contains {} peaks", spId, ttSpectrum.getMasses().length);
        System.exit(0);
    }

    private void fillmzDBPrecursor(Precursor mzdbPrecursor, fr.profi.brucker.timstof.model.Precursor timstofPrecursor, String collEnergy ){
        long start = System.currentTimeMillis();//-> VDS For timing logs
        IsolationWindowParamTree isolationParams = new IsolationWindowParamTree();
        List<CVParam> isolationParamsList = new ArrayList<>();
        isolationParamsList.add(buildCVParam("MS:1000827", "isolation window target m/z", String.valueOf(timstofPrecursor.getMonoIsotopicMz()), "MS"));
        isolationParams.setCvParams(isolationParamsList);
        mzdbPrecursor.setIsolationWindow(isolationParams);

        Activation activation = new Activation();
        List<CVParam> params = new ArrayList<>();
        //<cvParam cvRef="MS" accession="MS:1000133" name="collision-induced dissociation" value=""/>
        params.add(buildCVParam("MS:1000133", "collision-induced dissociation", "", "MS"));
        //<cvParam cvRef="MS" accession="MS:1000045" name="collision energy" value="30.0" unitCvRef="UO" unitAccession="UO:0000266" unitName="electronvolt"/>
        params.add(buildCVParam("MS:1000045", "collision energy", collEnergy, "MS"));
        activation.setCvParams(params);
        mzdbPrecursor.setActivation(activation);

        SelectedIonList ionList = new SelectedIonList();
        ionList.setCount(1);
        SelectedIon ion = new SelectedIon();
        List<SelectedIon> selectedIons = new ArrayList<>();
        List<CVParam> ionParams = new ArrayList<>();
        ionParams.add(buildCVParam("MS:1000744", "selected ion m/z", String.valueOf(timstofPrecursor.getMonoIsotopicMz()), "MS"));
        ionParams.add(buildCVParam("MS:1000041", "charge state", String.valueOf(timstofPrecursor.getCharge()), "MS"));
        ionParams.add(buildCVParam("MS:1000042", "peak intensity", String.valueOf(timstofPrecursor.getIntensity()), "MS"));
        ion.setCvParams(ionParams);
        selectedIons.add(ion);
        ionList.setSelectedIons(selectedIons);
        mzdbPrecursor.setSelectedIonList(ionList);
    }

    public static void main(String[] args) {
        Timstof2Mzdb inst = null;
        ConverterArguments convertArgs = new ConverterArguments();
        JCommander cmd =  JCommander.newBuilder().addObject(convertArgs).build();

        try {
            cmd.parse(args);
            String fileToConvert = filepath;
            SpectrumGeneratingMethod ms1Method = SpectrumGeneratingMethod.SMOOTH;
            if(convertArgs.filename != null) {
                LOG.info("File to convert: " + convertArgs.filename);
                fileToConvert = convertArgs.filename;
            } else
                LOG.info("NO File name set, use "+filepath);

            if(convertArgs.ms1 != null) {
                LOG.info("Ms1 set to " + convertArgs.ms1);
                ms1Method = convertArgs.ms1;
            } else
                LOG.info("NO specific ms1 convertion mode set, use default SMOOTH mode.");

            File ttDir = new File(fileToConvert);
            if(!ttDir.exists()){
                LOG.error("File "+fileToConvert+" does not exist !! ");
                System.exit(1);
            }

            inst = new Timstof2Mzdb(ttDir, ms1Method);
            inst.createMZdBData();

        } catch (ParameterException pe) {
            LOG.info("Error parsing arguments: "+pe.getMessage());
            cmd.usage();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if(inst != null) {
                LOG.info("Close file" );
                inst.closeFile();
            }
        }
    }
}
