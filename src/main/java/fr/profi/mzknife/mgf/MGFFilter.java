package fr.profi.mzknife.mgf;

import fr.profi.mgf.MGFConstants;
import fr.profi.ms.model.MSMSSpectrum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class MGFFilter extends MGFRewriter {
  private final static Logger LOG = LoggerFactory.getLogger(MGFFilter.class);

  List<Integer> charges;
  Boolean toExclude;

  public MGFFilter(File srcFile, File dstFile) throws IOException {
    super(srcFile,dstFile);

  }

  public void setExcludeCharges(List<Integer> charges2Exclude){
    charges = charges2Exclude;
    toExclude = true;
  }

  public void setCharges(List<Integer> charges2Keep){
    charges = charges2Keep;
    toExclude = false;
  }


  // Method that may be redefined in subclasses
  // default behaviour no filtering or value processing
  protected MSMSSpectrum getSpectrum2Export(MSMSSpectrum inSpectrum){
    boolean sameCharge =charges.contains(inSpectrum.getPrecursorCharge());
    if( (sameCharge && toExclude) || (!sameCharge && !toExclude)) {
      LOG.debug(" Spectrum {} is rejected.", inSpectrum.getAnnotation(MGFConstants.TITLE));
      return null;
    }else
      return inSpectrum;
  }

}
