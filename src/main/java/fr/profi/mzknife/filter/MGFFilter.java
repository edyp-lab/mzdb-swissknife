package fr.profi.mzknife.filter;

import fr.profi.mzknife.MGFRewriter;
import fr.profi.mzscope.InvalidMGFFormatException;
import fr.profi.mzscope.MGFConstants;
import fr.profi.mzscope.MSMSSpectrum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class MGFFilter extends MGFRewriter {
  private final static Logger LOG = LoggerFactory.getLogger(MGFFilter.class);

  Integer charge;
  Boolean toExclude;

  public MGFFilter(File srcFile, File dstFile) throws InvalidMGFFormatException {
    super(srcFile,dstFile);

  }

  public void setExcludeCharge(Integer charge2Exclude){
    charge = charge2Exclude;
    toExclude = true;
  }

  public void setCharge(Integer charge2Keep){
    charge = charge2Keep;
    toExclude = false;
  }


  // Method that may be redefined in subclasses
  // default behaviour no filtering or value processing
  protected MSMSSpectrum getSpectrum2Export(MSMSSpectrum inSpectrum){
    boolean sameCharge =charge.equals(inSpectrum.getPrecursorCharge());
    if( (sameCharge && toExclude) || (!sameCharge && !toExclude)) {
      LOG.debug(" Spectrum {} is rejected.", inSpectrum.getAnnotation(MGFConstants.TITLE));
      return null;
    }else
      return inSpectrum;
  }

}
