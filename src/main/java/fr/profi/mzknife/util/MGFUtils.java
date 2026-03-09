package fr.profi.mzknife.util;

import fr.profi.mgf.InvalidMGFFormatException;
import fr.profi.mgf.MGFConstants;
import fr.profi.mgf.MGFReader;
import fr.profi.ms.model.MSMSSpectrum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MGFUtils {

  private final static Logger LOG = LoggerFactory.getLogger(MGFUtils.class);
  private static final Pattern SCAN_PATTERN = Pattern.compile("scan\\W+([0-9]+)");
  private static final Pattern DTA_PATTERN = Pattern.compile("[^.]*\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.dta");


  public static String getScanAsString(MSMSSpectrum spectrum) {
    String scans = (String) spectrum.getAnnotation(MGFConstants.SCANS);
    if ((scans == null) || (scans.trim().isEmpty())) {
      return extractScanAsString((String)spectrum.getAnnotation(MGFConstants.TITLE));
    }
    return scans;
  }

  public static String extractScanAsString(String title) {
      String searchedTitle = title.toLowerCase().replace("index", "scan").replace("scans", "scan");
      Matcher m = SCAN_PATTERN.matcher(searchedTitle);
      if (m.find()) {
        String scanNumberStr = m.group(1);
        return scanNumberStr;
      } else {
        // try dta mode
        m = DTA_PATTERN.matcher(title.toLowerCase());
        if (m.find()) {
          String scanNumberStr = m.group(1);
          return scanNumberStr;
        } else {
          return null;
        }
      }
  }

  public static void main(String[] args) {

    File directory = new File("C:\\Local\\bruley\\Tests\\MGF\\Scan_Title");
    final File[] files = directory.listFiles((dir, name) -> name.endsWith(".mgf"));
    for (File file : files) {
      try {
        MGFReader reader = new MGFReader(file);
        final List<MSMSSpectrum> spectrumList = reader.readAllSpectrum();

//        String title = (String)spectrumList.get(0).getAnnotation(MGFConstants.TITLE);
//        final String scan = MGFUtils.extractScanAsString(title);
//        LOG.info("File = "+file.getName()+"; Title = "+title+" -> "+scan);

      for(MSMSSpectrum spectrum : spectrumList) {
          String scan = getScanAsString(spectrum);
          LOG.info("File = "+file.getName()+"; Title = "+(String)spectrum.getAnnotation(MGFConstants.TITLE)+" -> "+scan);
        }
      } catch (InvalidMGFFormatException | IOException e) {
        throw new RuntimeException(e);
      }

    }

  }
}
