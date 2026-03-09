package fr.profi.mzknife.mgf;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.io.reader.iterator.SpectrumIterator;
import fr.profi.mzdb.io.writer.mgf.IPrecursorComputation;
import fr.profi.mzdb.io.writer.mgf.ISpectrumProcessor;
import fr.profi.mzdb.io.writer.mgf.MgfPrecursor;
import fr.profi.mzdb.io.writer.mgf.MgfWriter;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.Spectrum;
import fr.profi.mzdb.model.SpectrumData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MGFThreadedWriter extends MgfWriter {

  final Logger logger = LoggerFactory.getLogger(MGFThreadedWriter.class);

  static class Tuple {

    MgfPrecursor precursor;
    Spectrum spectrum;
    DataEncoding dataEncoding;
    ISpectrumProcessor spectrumProcessor;
    float intensityCutoff;
    String spectrumTitle;

    public Tuple(MgfPrecursor precursor, Spectrum s, DataEncoding dataEnc, ISpectrumProcessor spectrumProcessor, float intensityCutoff, String spectrumTitle) {
      this.precursor = precursor;
      this.spectrum = s;
      this.dataEncoding = dataEnc;
      this.spectrumProcessor = spectrumProcessor;
      this.intensityCutoff = intensityCutoff;
      this.spectrumTitle = spectrumTitle;
    }
  }
  protected BlockingQueue<Optional<String>> outputQueue;
  protected BlockingQueue<Optional<Tuple>> processQueue;
  protected Thread writerThread;
  protected List<Thread> workers;
  protected int workersCount = 3;

  public MGFThreadedWriter(String mzDBFilePath, int msLevel, String prolineSpectraTitleSeparator) throws SQLiteException, FileNotFoundException {
    super(mzDBFilePath, msLevel, prolineSpectraTitleSeparator);
  }

  public MGFThreadedWriter(String mzDBFilePath, int msLevel) throws SQLiteException, FileNotFoundException {
    super(mzDBFilePath, msLevel);
  }

  public MGFThreadedWriter(String mzDBFilePath) throws SQLiteException, FileNotFoundException {
    super(mzDBFilePath);
  }

  private void initThreads(int workersCount, PrintWriter mgfWriter) {
    processQueue = new LinkedBlockingQueue<>(100);
    outputQueue = new LinkedBlockingQueue<>(100);
    workers = new ArrayList<>(workersCount);
    for (int i = 0; i < workersCount; i++) {
      workers.add(startProcessWorker(i));
    }
    startWriter(mgfWriter);
  }

  private Thread startProcessWorker(int id) {

    Thread worker = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Optional<Tuple> tupleOpt;

          while ((tupleOpt = processQueue.take()).isPresent()) {

            Tuple t = tupleOpt.get();
            final SpectrumData processedSpectrumData = t.spectrumProcessor.processSpectrum(t.precursor, t.spectrum.getData());
            String spectrumAsStr = stringifySpectrum(t.precursor, new Spectrum(t.spectrum.getHeader(), processedSpectrumData), t.dataEncoding, t.intensityCutoff, t.spectrumTitle);
            outputQueue.put(Optional.of(spectrumAsStr));
          }

          logger.info("worker thread terminate");
          processQueue.put(Optional.empty());

        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      }
    });
    worker.setName("SpectrumProcessor Worker Thread #"+ id);
    worker.start();

    return worker;
  }

  private void startWriter(PrintWriter mgfWriter) {

    writerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Optional<String> stringOpt;

          while ((stringOpt = outputQueue.take()).isPresent()) {
            mgfWriter.println(stringOpt.get());
            mgfWriter.println();
          }

          mgfWriter.flush();
          logger.info("Writer thread terminate");

        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      }
    });

    writerThread.setName("MGF Writer Thread");
    writerThread.start();
  }

  @Override
  public void write(String mgfFile, IPrecursorComputation precComp, ISpectrumProcessor spectrumProcessor, float intensityCutoff, boolean exportProlineTitle) throws SQLiteException, IOException {

    if(!mzDbReader.getConnection().isOpen()) {
      initReader();
    }

    // treat path mgfFile ?
    if (mgfFile.isEmpty())
      mgfFile = this.mzDBFilePath + ".mgf";

    // Configure the mzDbReader in order to load all precursor lists and all scan list when reading spectra headers
    mzDbReader.enablePrecursorListLoading();
    mzDbReader.enableScanListLoading();

    // Iterate MSn spectra
    final Iterator<Spectrum> spectrumIterator = new SpectrumIterator(mzDbReader, mzDbReader.getConnection(), msLevel);
    final PrintWriter mgfWriter = new PrintWriter(new BufferedWriter(new FileWriter(mgfFile)));
    final Map<Long, DataEncoding> dataEncodingBySpectrumId = this.mzDbReader.getDataEncodingBySpectrumId();
    final String rawFilename = mzDbReader.getFirstSourceFileName().split("\\.")[0];

    if (headerComments != null && !headerComments.isEmpty()) {
      headerComments.forEach(l -> mgfWriter.println("# " + l));
      mgfWriter.println();
    }

    initThreads(workersCount, mgfWriter);

    int spectraCount = 0;
    int precursorsCount = 0;
    int ignoredSpectraCount = 0;

    try {
      while (spectrumIterator.hasNext()) {

        Spectrum s = spectrumIterator.next();
        long spectrumId = s.getHeader().getId();
        DataEncoding dataEnc = dataEncodingBySpectrumId.get(spectrumId);

        MgfPrecursor[] precursors = precComp.getMgfPrecursors(mzDbReader, s.getHeader());
        for (int k = 0; k < precursors.length; k++) {
          String spectrumTitle = getTitle(precursors[k], s.getHeader(), rawFilename, exportProlineTitle);
          processQueue.put(Optional.of(new Tuple(precursors[k], s, dataEnc, spectrumProcessor, intensityCutoff, spectrumTitle)));
          precursorsCount++;
        }
        if (precursors.length > 0) {
          spectraCount++;
        } else {
          ignoredSpectraCount++;
        }
      }

      logger.info("put empty to terminate writer thread");
      logger.info("Waiting for writer thread");
      processQueue.put(Optional.empty());

      for(Thread worker : workers) {
        worker.join();
      }

      outputQueue.put(Optional.empty());
      writerThread.join();
      this.logger.info("MGF file successfully created: {} precursors exported from {} spectra. {} spectra ignored.", precursorsCount, spectraCount, ignoredSpectraCount);

    } catch (InterruptedException ie) {
      logger.info("Threaded writer failure");
      throw new RuntimeException(ie);
    }

    mzDbReader.close();
    mgfWriter.flush();
    mgfWriter.close();
  }

  public void setWorkersCount(int workersCount) {
    this.workersCount = workersCount;
  }

}
