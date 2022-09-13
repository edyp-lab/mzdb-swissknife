package fr.profi.mzknife.mzdb;

import fr.profi.mzdb.BBSizes;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.MzDBMetaData;
import fr.profi.mzdb.model.SpectrumMetaData;
import fr.profi.mzdb.io.util.MzDBUtil;
import fr.profi.mzdb.io.writer.MzDBWriter;
import fr.profi.mzdb.model.*;
import fr.profi.mzknife.util.RecalibrateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Recalibrate a mzdb file
 */
public class MzDBRecalibrator {

  private final static Logger LOG = LoggerFactory.getLogger(MzDBRecalibrator.class);

  private final MzDbReader m_srcReader;
  private final File m_dstFile;


  public MzDBRecalibrator(MzDbReader srcReader, File dstFile)  {
    m_dstFile = dstFile;
    m_srcReader = srcReader;
    m_srcReader.enableParamTreeLoading();
    m_srcReader.enablePrecursorListLoading();
    m_srcReader.enableScanListLoading();
    m_srcReader.enableDataStringCache();

  }


  public void recalibrate(Long firstScan, Long lastScan, double deltaMass) {
    MzDBWriter writer = null;

    try {

      BBSizes defaultBBsize = new BBSizes(5,10000,15,0);

      MzDBMetaData mzDbMetaData = MzDBUtil.createMzDbMetaData(m_srcReader);
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
