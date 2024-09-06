package fr.profi.mzknife.mzdb;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.IsolationWindowParamTree;
import fr.profi.mzdb.db.model.params.Precursor;
import fr.profi.mzdb.db.model.params.param.CVEntry;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.io.writer.mgf.MgfBoostPrecursorExtractor;
import fr.profi.mzdb.model.*;
import fr.profi.mzknife.util.ParamsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MzDBMetrics {

  private static final CharSequence DELIMITER = "\t";

  final File m_inputMzdbFile;

  final File m_outputFile;
  MzDbReader m_mzDbReader;

  private final static Logger LOG = LoggerFactory.getLogger(MzDBMetrics.class);

  public MzDBMetrics(File inputMzdbFile, File outputFile) {
    this.m_inputMzdbFile = inputMzdbFile;
    this.m_outputFile = outputFile;
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

  public void computeMetrics() {

    LOG.info(" Compute mzDB metrics "+m_inputMzdbFile.getName());

    try {
      if(!m_mzDbReader.getConnection().isOpen())
        initReader();

      BufferedWriter writer = new BufferedWriter(new FileWriter(m_outputFile));
      String[] columns = { "scan.id", "scan.rt", "master_scan.id", "master_scan.rt", "master_scan.peaks_count", "samecycle_scan.id", "samecycle_scan.rt", "samecycle_scan.peaks_count", "nearest_scan.id", "nearest_scan.rt", "nearest_scan.peaks_count"};

      writer.write(Arrays.stream(columns).collect(Collectors.joining(DELIMITER)));
      writer.newLine();

      final SpectrumHeader[] ms2SpectrumHeaders = m_mzDbReader.getMs2SpectrumHeaders();
      for (SpectrumHeader spHeader : ms2SpectrumHeaders) {
        final IsolationWindow iw = retrieveIsolationWindow(spHeader);
        final List<Optional<SpectrumSlice>> ms1DataList = retrieveMS1Data(spHeader, iw);

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(spHeader.getSpectrumId()).append(DELIMITER);
        strBuilder.append(spHeader.getElutionTime()).append(DELIMITER);


        for (Optional<SpectrumSlice> spectrumSlice : ms1DataList) {
          if (spectrumSlice.isPresent()) {
            strBuilder.append(spectrumSlice.get().getSpectrumId()).append(DELIMITER);
            strBuilder.append(spectrumSlice.get().getHeader().getElutionTime()).append(DELIMITER);
            strBuilder.append(countSWPeaks(spectrumSlice.get(), iw)).append(DELIMITER);

          } else {
            strBuilder.append("NA").append(DELIMITER);
            strBuilder.append("NA").append(DELIMITER);
            strBuilder.append("NA").append(DELIMITER);
          }
        }

        writer.write(strBuilder.toString());
        writer.newLine();
        writer.flush();

      }

    } catch(SQLiteException | IOException e){
        e.printStackTrace();
        m_mzDbReader.close();
    }

  }


  private IsolationWindow retrieveIsolationWindow(SpectrumHeader spectrumHeader) {

    Precursor precursor = spectrumHeader.getPrecursor();

    IsolationWindowParamTree iw = precursor.getIsolationWindow();
    if (iw == null) return null;

    CVEntry[] cvEntries = { CVEntry.ISOLATION_WINDOW_LOWER_OFFSET, CVEntry.ISOLATION_WINDOW_TARGET_MZ, CVEntry.ISOLATION_WINDOW_UPPER_OFFSET };
    CVParam[] cvParams = iw.getCVParams(cvEntries);

    float lowerMzOffset = Float.parseFloat(cvParams[0].getValue());
    float targetMz = Float.parseFloat(cvParams[1].getValue());
    float upperMzOffset = Float.parseFloat(cvParams[2].getValue());
    float minmz = targetMz - lowerMzOffset;
    float maxmz = targetMz + upperMzOffset;

    return new IsolationWindow(minmz, maxmz);
  }

  private List<Optional<SpectrumSlice>> retrieveMS1Data(SpectrumHeader spectrumHeader, IsolationWindow isolationWindow) throws StreamCorruptedException, SQLiteException {

    float time = spectrumHeader.getElutionTime();
    double minmz = isolationWindow.getMinMz();
    double maxmz = isolationWindow.getMaxMz();
    float minrt = time - 5;
    float maxrt = time + 5;

    UserParam masterScanUP = spectrumHeader.getScanList().getScans().get(0).getUserParam(ParamsHelper.UP_MASTER_SCAN_NAME);
    Optional<SpectrumSlice> masterScanOpt = Optional.empty();
    if (masterScanUP != null) {
      int masterScanIndex = Integer.parseInt(masterScanUP.getValue());
      if (masterScanIndex >= 0) {
        Spectrum masterScan = m_mzDbReader.getSpectrum(masterScanIndex);
        masterScanOpt = Optional.of(new SpectrumSlice(masterScan.getHeader(), masterScan.getData().mzRangeFilter(minmz - 5, maxmz + 5)));
      }
    }

    SpectrumSlice[] spectrumSlices = m_mzDbReader.getMsSpectrumSlices(minmz - 5.0, maxmz + 5.0, minrt, maxrt);
    IonMobilityMode ionMobilityMode = m_mzDbReader.getIonMobilityMode();
    boolean hasIonMobility = (ionMobilityMode != null) && ionMobilityMode.getIonMobilityType().equals(IonMobilityType.FAIMS);

    String cv = (hasIonMobility) ? MgfBoostPrecursorExtractor.readIonMobilityCV(spectrumHeader).get() : null;

    final Optional<SpectrumSlice> sameCycleScanOpt = Arrays.stream(spectrumSlices).filter(s -> (s.getHeader().getCycle() == spectrumHeader.getCycle()) && (!hasIonMobility || cv == MgfBoostPrecursorExtractor.readIonMobilityCV(s.getHeader()).get())).findFirst();

    final Stream<SpectrumSlice> spectrumSliceStream = Arrays.stream(spectrumSlices).filter(s -> (!hasIonMobility || cv == MgfBoostPrecursorExtractor.readIonMobilityCV(s.getHeader()).get()));
    final Optional<SpectrumSlice> nearestScanOpt = spectrumSliceStream.min((o1, o2) -> Double.compare(Math.abs(o1.getHeader().getElutionTime() - time), Math.abs(o2.getHeader().getElutionTime() - time)));

    return Arrays.asList(masterScanOpt, sameCycleScanOpt, nearestScanOpt);
  }

  private long countSWPeaks(SpectrumSlice spectrum, IsolationWindow iw) {
    return Arrays.stream(spectrum.toPeaks()).filter(p -> (p.getMz() >= iw.getMinMz()) && (p.getMz() <= iw.getMaxMz())).count();
  }
}
