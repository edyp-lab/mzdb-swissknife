package fr.profi.mzknife.mgf;

import fr.profi.ms.model.MSMSSpectrum;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MGFThreadedRewriter extends MGFRewriter {

  protected BlockingQueue<Optional<MSMSSpectrum>> inputQueue;
  protected BlockingQueue<Optional<String>> outputQueue;
  protected Thread writerThread;
  protected List<Thread> workers;
  protected int workersCount = 3;

  public MGFThreadedRewriter() {
    super();
  }

  public MGFThreadedRewriter(File srcFile, File m_dstFile) throws IOException {
    super(srcFile, m_dstFile);
  }

  private void initThreads(int workersCount) {
    inputQueue = new LinkedBlockingQueue<>(100);
    outputQueue = new LinkedBlockingQueue<>(20);
    workers = new ArrayList<>(workersCount);
    for (int i = 0; i < workersCount; i++) {
      workers.add(startProcessWorker(i));
    }
    startWriter();
  }

  private Thread startProcessWorker(int id) {

    Thread worker = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Optional<MSMSSpectrum> spectrum;

          while ((spectrum = inputQueue.take()).isPresent()) {
            MSMSSpectrum outSpectrum = getSpectrum2Export(spectrum.get());
            if (outSpectrum != null) {
              String spectrumAsStr = MGFWriter.stringifySpectrum(outSpectrum);
              outputQueue.put(Optional.of(spectrumAsStr));
            }
          }

          LOG.info("worker thread terminate");
          inputQueue.put(Optional.empty());

        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      }
    });
    worker.setName("MGF Worker Thread #"+ id);
    worker.start();

    return worker;
  }

  private void startWriter() {

    writerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Optional<String> stringOpt;
          PrintWriter mgfWriter = new PrintWriter(new BufferedWriter(new FileWriter(m_dstFile)));

          while ((stringOpt = outputQueue.take()).isPresent()) {
            mgfWriter.print(stringOpt.get());
            mgfWriter.println();
          }

          LOG.info("Writer thread terminate");
          mgfWriter.flush();
          mgfWriter.close();

        } catch (IOException|InterruptedException e) {
          e.printStackTrace();
        }

      }
    });

    writerThread.setName("MGF Writer Thread");
    writerThread.start();
  }

  public void rewriteMGF() throws IOException {

    initThreads(workersCount);

    try {
      while (m_spectraIterator.hasNext()) {
        MSMSSpectrum spectrum = m_spectraIterator.next();
        inputQueue.put(Optional.of(spectrum));
      }
      LOG.info("put empty to terminate writer thread");
      LOG.info("Waiting for writer thread");
      inputQueue.put(Optional.empty());

      for(Thread worker : workers) {
        worker.join();
      }

      outputQueue.put(Optional.empty());
      writerThread.join();
      LOG.info("Done");
    } catch (InterruptedException e) {
        LOG.info("fail");
        throw new RuntimeException(e);
    }
  }

  public void setWorkersCount(int workersCount) {
    this.workersCount = workersCount;
  }

}
