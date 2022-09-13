package fr.profi.mzknife.mgf;

import fr.profi.mzscope.InvalidMGFFormatException;
import fr.profi.mzscope.MGFReader;
import fr.profi.mzscope.MSMSSpectrum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.List;

public class MGFRewriter {

  protected final List<MSMSSpectrum> m_msmsSpectra;
  protected final File m_dstFile;

  private final static Logger LOG = LoggerFactory.getLogger(MGFRewriter.class);

  protected MGFRewriter() {
    this.m_msmsSpectra = Collections.emptyList();
    this.m_dstFile = null;
  }

  public MGFRewriter(File srcFile, File m_dstFile) throws InvalidMGFFormatException {
    MGFReader reader = new MGFReader();
    this.m_msmsSpectra = reader.read(srcFile);
    this.m_dstFile = m_dstFile;
  }

  public void rewriteMGF() throws IOException {

    PrintWriter mgfWriter = new PrintWriter(new BufferedWriter(new FileWriter(m_dstFile)));
    for (MSMSSpectrum spectrum : m_msmsSpectra) {
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
