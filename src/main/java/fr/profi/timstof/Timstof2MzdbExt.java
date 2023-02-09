package fr.profi.timstof;

import com.almworks.sqlite4java.SQLiteException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import fr.profi.bruker.timstof.model.AbstractTimsFrame;
import fr.profi.bruker.timstof.model.SpectrumGeneratingMethod;
import fr.profi.bruker.timstof.model.TimsMSFrame;
import fr.profi.bruker.timstof.model.TimsPASEFFrame;
import fr.profi.mzdb.BBSizes;
import fr.profi.mzdb.db.model.params.Precursor;
import fr.profi.mzdb.io.writer.MzDBWriter;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.MzDBMetaData;
import fr.profi.mzdb.model.Spectrum;
import fr.profi.mzdb.model.SpectrumMetaData;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

public class Timstof2MzdbExt extends Timstof2Mzdb {

  private final static Logger LOG = LoggerFactory.getLogger(Timstof2MzdbExt.class);

  private static final StopWatch readFrameSW = new StopWatch("read frame");

  DataEncoding m_centroid3dDataEncoding;
  private final MobilityRepresentationMethod mobilityMethod;

  public Timstof2MzdbExt(File ttFile, SpectrumGeneratingMethod ms1Method, MobilityRepresentationMethod mobilityMethod) {
    super(ttFile, ms1Method);
    this.mobilityMethod = mobilityMethod;
    // TODO use mzdb-access ion_mobility branch to enable this feature
    //    m_centroid3dDataEncoding = new DataEncoding(-1, DataMode.CENTROID_3D, PeakEncoding.HIGH_RES_PEAK, "none", ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  protected void initFramesData(){
    m_precursorByIds = m_ttReader.getPrecursorInfoById(m_fileHdl);
  }

  @Override
  protected void createMZdBData(){
    MzDBWriter writer = null;

    try {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH_mm_ss");
      String date = dateFormat.format(Calendar.getInstance().getTime());
      String outFilePath = m_ttFile.getAbsolutePath() +"_"+date+".mzdb";
      File outFile = new File( outFilePath);
      BBSizes defaultBBsize = new BBSizes(5, 10000, 15, 0);

      MzDBMetaData mzDbMetaData =  createMzDbMetaData();
      writer = new MzDBWriter(outFile, mzDbMetaData, /*newNBbSize*/ defaultBBsize, false);
      writer.initialize();

      int spId = 1; //Spectrum Index start at 1
      int mzDBSpId = spId;
      int cycle =0; //VDS : TODO 1 cycle = 1Ms + xMSMS ?

      //--> VDS-TIME: to get duration debug info
      long time_readFrame = 0;
      long time_ms2 = 0;
//            long time_fillPrec = 0;
      long time_ms1 = 0;
      long time_write = 0;

      DataEncoding ms1Encoding = m_centroidDataEncoding;
      switch (mobilityMethod) {
        case NONE:
        case PER_SCAN:
          ms1Encoding = m_centroidDataEncoding;
          break;
        case PER_PEAK:
          ms1Encoding = m_centroid3dDataEncoding;
      }

      List<AbstractTimsFrame> frames = m_ttReader.getFullTimsFrames(m_fileHdl);
      Collections.sort(frames);

      for(int frameIdx = 0; frameIdx < frames.size(); frameIdx++) {

        AbstractTimsFrame timsFrame = frames.get(frameIdx);
        long start = System.currentTimeMillis();  //--> VDS-TIME: to get duration debug info

        if (!timsFrame.spectrumRead()) {
          //get spectrum information if not read yet
          List<AbstractTimsFrame> tfs = Collections.singletonList(timsFrame);
          m_ttReader.fillFramesWithSpectrumData(m_fileHdl, tfs);
        }

        Precursor mzdbPrecursor = null;
        fr.profi.bruker.timstof.model.Spectrum ttSpectrum = null;
        Double preMz = null;
        Integer preCharge = null;
        fr.profi.mzdb.model.Spectrum mzdbSp = null;

        //--> VDS-TIME: to get duration debug info
        long step1 = System.currentTimeMillis();
        time_readFrame += step1-start; //--> VDS-TIME: Ecoli (10Go) ~30min
        long step2;

        float rtInSec = (float) timsFrame.getTime();

        switch (timsFrame.getMsmsType()) {
          case MS:
            // Ms Frame
            cycle++;
            if (mobilityMethod == MobilityRepresentationMethod.NONE) {
              //Read 'single' spectrum ==> TODO change to read all Scans Spectrum or use groups ??
              ttSpectrum = timsFrame.getSingleSpectrum(m_ms1Method);
              //--> VDS-TIME: to get duration debug info
              step2 = System.currentTimeMillis();
              time_ms1 += step2 - step1;  //--> VDS-TIME: Ecoli (10Go) ~10min
              //                                //--> VDS-TIME: to get duration debug info
              mzdbSp = buildMzdbSpectrum(ttSpectrum, mzDBSpId, cycle, rtInSec, 1, timsFrame.getSummedIntensity(), null, null, null);
              if (mzdbSp != null) {
                mzDBSpId = writeMzdbSpectrum(writer, timsFrame.getMsmsType(), mzdbSp, ms1Encoding);
                long step4 = System.currentTimeMillis();
                time_write += step4 - step2;//--> VDS-TIME: Ecoli (10Go) ~4.5min
              }
            } else if (mobilityMethod == MobilityRepresentationMethod.PER_SCAN) {
              for (int scanIdx = 1; scanIdx < timsFrame.getNbrScans(); scanIdx++) {
                ttSpectrum = ((TimsMSFrame) timsFrame).getScanSpectrum(scanIdx);
                double deltaRt = (frameIdx < frames.size()-1) ? (frames.get(frameIdx+1).getTime() - timsFrame.getTime())/timsFrame.getNbrScans() : 1.0/timsFrame.getNbrScans();

                if (ttSpectrum != null) {
                  //--> VDS-TIME: to get duration debug info
                  step2 = System.currentTimeMillis();
                  time_ms1 += step2 - step1;  //--> VDS-TIME: Ecoli (10Go) ~10min
                  mzdbSp = buildMzdbSpectrum(ttSpectrum, mzDBSpId, cycle, (float)(rtInSec + scanIdx*deltaRt), 1, timsFrame.getSummedIntensity(), null, null, null);
                  if (mzdbSp != null) {
                    mzDBSpId = writeMzdbSpectrum(writer, timsFrame.getMsmsType(), mzdbSp, ms1Encoding);
                    long step4 = System.currentTimeMillis();
                    time_write += step4 - step2;//--> VDS-TIME: Ecoli (10Go) ~4.5min
                  }
                }
              }
            } else if (mobilityMethod == MobilityRepresentationMethod.PER_PEAK) {
// TODO use mzdb-access ion_mobility branch to enable this feature
//              List<Peak3D> peakList = new ArrayList<>();
//              for (int scanIdx = 1; scanIdx < timsFrame.getNbrScans(); scanIdx++) {
//                ttSpectrum = ((TimsMSFrame) timsFrame).getScanSpectrum(scanIdx);
//
//                if (ttSpectrum != null) {
//                  double[] masses = ttSpectrum.getMasses();
//                  float[] intensities = ttSpectrum.getIntensities();
//                  for (int k = 0; k < ttSpectrum.getMasses().length; k++) {
//                    peakList.add(new Peak3D(masses[k], intensities[k], (short)scanIdx));
//                  }
//                }
//              }
//              Collections.sort(peakList);
//              step2 = System.currentTimeMillis();
//              time_ms1 += step2 - step1;  //--> VDS-TIME: Ecoli (10Go) ~10min
//              //                                //--> VDS-TIME: to get duration debug info
//              mzdbSp = buildMzdbSpectrum(peakList, "Frame_" + timsFrame.getId(), mzDBSpId, cycle, rtInSec, 1, timsFrame.getSummedIntensity(), null, null, null);
//              if (mzdbSp != null) {
//                mzDBSpId = writeMzdbSpectrum(writer, timsFrame.getMsmsType(), mzdbSp, ms1Encoding);
//                long step4 = System.currentTimeMillis();
//                time_write += step4 - step2;//--> VDS-TIME: Ecoli (10Go) ~4.5min
//              }
            }
            break;

          case PASEF:
            if( ((TimsPASEFFrame)timsFrame).getPrecursorIds() != null){
              List<Integer> precursorIds = ((TimsPASEFFrame)timsFrame).getPrecursorIds();
              Collections.sort(precursorIds);

              for (int precursorIdx = 0; precursorIdx < precursorIds.size(); precursorIdx++) {

                int precursorId = precursorIds.get(precursorIdx);
                ttSpectrum = ((TimsPASEFFrame)timsFrame).getPrecursorSpectrum(precursorId);
                if (ttSpectrum == null) {
                  LOG.warn("#### WARNING #### No precursor spectrum was defined for frame  " + timsFrame.getId() + "; precursorId : " + precursorId + ". Spectrum index " + spId);
                } else {

                  //Create mzDB Precursor using timstof Precursor as model
//                                    long step21 = System.currentTimeMillis();
//                                    time_fillPrec += step21-step1;
                  fr.profi.bruker.timstof.model.Precursor timstofPrecursor = m_precursorByIds.get(precursorId);
                  mzdbPrecursor = new Precursor();
                  mzdbPrecursor.setSpectrumRef(ttSpectrum.getTitle());

                  if (timstofPrecursor!=null) {
                    //fillmzDBPrecursor(mzdbPrecursor, timstofPrecursor, String.valueOf(timsFrame.getPrecursorCollisionEnergy(precursorId)));
                    preMz = timstofPrecursor.getMonoIsotopicMz();
                    preCharge = timstofPrecursor.getCharge();
                  }

                  //--> VDS-TIME: to get duration debug info
                  step2 = System.currentTimeMillis();
                  time_ms2 += step2 - step1; //--> VDS-TIME: Ecoli (10Go) ~1.5s
                  mzdbSp = buildMzdbSpectrum(ttSpectrum, mzDBSpId, cycle, rtInSec + (precursorIdx * RT_EPSILON), 2, timsFrame.getSummedIntensity(), preMz, preCharge, mzdbPrecursor);
                  if (mzdbSp != null) {
                    mzDBSpId = writeMzdbSpectrum(writer, timsFrame.getMsmsType(), mzdbSp, m_centroidDataEncoding);
                    long step4 = System.currentTimeMillis();
                    time_write += step4 - step2;//--> VDS-TIME: Ecoli (10Go) ~4.5min
                  }
                }
              }


            } else {
              LOG.warn("#### WARNING ####  UNSUPPORTED Frame type. Only MS1 and PASEF are supported actually. Frame "+timsFrame.getId()+" is getMsmsType "+timsFrame.getMsmsType());
              timsFrame.clearSpectraData();
              continue;
            }
            break;
          case DIA:
          case MSMS:
          default:
            LOG.warn("#### WARNING ####  UNSUPPORTED Frame type. Only MS1 and PASEF are supported actually. Frame "+timsFrame.getId()+" is getMsmsType "+timsFrame.getMsmsType());
            timsFrame.clearSpectraData();
            continue;
        }

        timsFrame.clearSpectraData();
        spId++;

        //--> VDS-TIME: to get duration debug info
        if (spId % 1000 == 0) {
          LOG.info("Already written {} ({} in mzdb) spectra over {?} ", spId, mzDBSpId);
          LOG.debug("Time used to read Frame: " + time_readFrame);
          LOG.debug("Time used to create MS2: " + time_ms2);
          //LOG.info("Time used to create FILLPred: " + time_fillPrec);
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

//TODO use mzdb-access ion_mobility branch to enable this feature
//  private Spectrum buildMzdbSpectrum(List<Peak3D> peakList, String title, int mzDBSpId, int cycle, float rtInSec, int mslevel, int tic, Double precMz, Integer precCharge, Precursor precursor) {
//
//    double[] masses = new double[peakList.size()];
//    float[] intensities = new float[peakList.size()];
//    short[] mobilityIndexes = new short[peakList.size()];
//
//    for (int k = 0; k < peakList.size(); k++) {
//      Peak3D peak = peakList.get(k);
//      masses[k] = peak.mass;
//      intensities[k] = peak.intensity;
//      mobilityIndexes[k] = peak.mobilityIndex;
//    }
//
//    if (intensities.length > 0) { //At least one peak ... VDS TODO:  or create an empty spectrum ?
//      int maxIndex = 0;
//      float prevIntensity = intensities[0];
//      for (int index = 1; index < intensities.length; index++) {
//        float intAtIndex = intensities[index];
//        if (intAtIndex > prevIntensity) {
//          maxIndex = index;
//          prevIntensity = intAtIndex;
//        }
//      }
//
//      SpectrumHeader spH = new SpectrumHeader((long) mzDBSpId, mzDBSpId, cycle, rtInSec, mslevel, title, peakList.size(), false, tic, masses[maxIndex], intensities[maxIndex], precMz, precCharge, mzDBSpId, null);
//      spH.setPrecursor(precursor);
//      SpectrumData spData = new SpectrumData(masses, intensities, null, null, mobilityIndexes);
//      fr.profi.mzdb.model.Spectrum mzdbSp = new fr.profi.mzdb.model.Spectrum(spH, spData);
//
//      return mzdbSp;
//
//    } else {
//      LOG.info("mzdb scan id {} has no peaks ! It will not be written in the mzdb outputfile", mzDBSpId);
//      return null;
//    }
//  }

  private int writeMzdbSpectrum(MzDBWriter writer, AbstractTimsFrame.MsMsType msmsType, Spectrum mzdbSp, DataEncoding encoding) throws SQLiteException {

    long mzdbSpId = mzdbSp.getHeader().getSpectrumId();
    SpectrumMetaData spectrumMetaData = new SpectrumMetaData(mzdbSpId, "", "", "");
    writer.insertSpectrum(mzdbSp, spectrumMetaData, encoding);
    mzdbSpId++;
    return (int)mzdbSpId;
  }

  public static void main(String[] args) {
    Timstof2MzdbExt inst = null;
    ConverterArguments convertArgs = new ConverterArguments();
    JCommander cmd =  JCommander.newBuilder().addObject(convertArgs).build();


    try {
      cmd.parse(args);

      File ttDir = new File(convertArgs.filename);
      if(!ttDir.exists()){
        LOG.error("File "+convertArgs.filename+" does not exist !! ");
        System.exit(1);
      }

      inst = new Timstof2MzdbExt(ttDir, convertArgs.ms1, convertArgs.mobilityMethod);
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
