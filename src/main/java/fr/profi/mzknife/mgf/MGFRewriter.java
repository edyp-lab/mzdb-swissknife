package fr.profi.mzknife.mgf;

import fr.profi.mzscope.MGFReader;
import fr.profi.mzscope.MSMSSpectrum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.Iterator;

public class MGFRewriter {

  protected final static Logger LOG = LoggerFactory.getLogger(MGFRewriter.class);

  protected final File m_dstFile;
  protected final Iterator<MSMSSpectrum> m_spectraIterator;

  protected MGFRewriter() {
    this.m_dstFile = null;
    this.m_spectraIterator = Collections.emptyIterator();
  }

  public MGFRewriter(File srcFile, File m_dstFile) throws IOException {
    MGFReader reader = new MGFReader(srcFile);
    this.m_spectraIterator = reader;
    this.m_dstFile = m_dstFile;
  }

  public void rewriteMGF() throws IOException {

    PrintWriter mgfWriter = new PrintWriter(new BufferedWriter(new FileWriter(m_dstFile)));
    while (m_spectraIterator.hasNext()) {
      MSMSSpectrum spectrum = m_spectraIterator.next();
      MSMSSpectrum outSpectrum =  getSpectrum2Export(spectrum);
      if(outSpectrum != null) {
        String spectrumAsStr = MGFWriter.stringifySpectrum(outSpectrum);
        mgfWriter.print(spectrumAsStr);
        mgfWriter.println();
      }
    }

    mgfWriter.flush();
    mgfWriter.close();
  }


  // Method that may be redefined in subclasses
  // default behaviour no filtering or value processing
  protected MSMSSpectrum getSpectrum2Export(MSMSSpectrum inSpectrum){
    return inSpectrum;
  }


}
