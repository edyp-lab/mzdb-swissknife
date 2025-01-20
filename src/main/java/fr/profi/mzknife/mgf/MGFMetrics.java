package fr.profi.mzknife.mgf;

import fr.profi.mzknife.util.MGFUtils;
import fr.profi.mzscope.InvalidMGFFormatException;
import fr.profi.mzscope.MGFReader;
import fr.profi.mzscope.MSMSSpectrum;
import fr.profi.mzscope.Peak;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class MGFMetrics {

  private static final CharSequence DELIMITER = "\t";
  private static final DecimalFormat FORMAT_5_DIGITS = new DecimalFormat("0.00000", new DecimalFormatSymbols(Locale.US));
  private static final DecimalFormat FORMAT_1_DIGIT = new DecimalFormat("0.0", new DecimalFormatSymbols(Locale.US));
  protected final List<MSMSSpectrum> m_msmsSpectra;
  protected final File m_dstFile;

  private final static double MOZ_THRESHOLD = 136.0;

  protected MGFMetrics() {
    this.m_msmsSpectra = Collections.emptyList();
    this.m_dstFile = null;
  }

  public MGFMetrics(File srcFile, File dstFile) throws InvalidMGFFormatException, IOException {
    MGFReader reader = new MGFReader(srcFile);
    this.m_msmsSpectra = reader.readAllSpectrum();
    this.m_dstFile = dstFile;
  }

  public void dumpMGFMetrics() throws IOException {

    BufferedWriter writer = new BufferedWriter(new FileWriter(m_dstFile));
    String[] columns = { "scan", "mz", "charge", "intensity", "fragment_peaks_count", "reporters_intensity_ratio"};

    writer.write(Arrays.stream(columns).collect(Collectors.joining(DELIMITER)));
    writer.newLine();

    for (MSMSSpectrum spectrum : m_msmsSpectra) {

      StringBuilder strBuilder = new StringBuilder();
      strBuilder.append(MGFUtils.getScanAsString(spectrum)).append(DELIMITER);
      strBuilder.append(FORMAT_5_DIGITS.format(spectrum.getPrecursorMass())).append(DELIMITER);
      strBuilder.append(spectrum.getPrecursorCharge()).append(DELIMITER);
      strBuilder.append(FORMAT_1_DIGIT.format(spectrum.getPrecursorIntensity())).append(DELIMITER);
      strBuilder.append(spectrum.peaksCount()).append(DELIMITER);

      double lowMassRangeIntensity = 0.0;
      double highMassRangeIntensity = 0.0;
              
      for (Peak p : spectrum.getPeaks()) {
        if (p.getMz() < MOZ_THRESHOLD) {
          lowMassRangeIntensity = Math.max(p.getIntensity(), lowMassRangeIntensity);
        } else {
          highMassRangeIntensity = Math.max(p.getIntensity(), highMassRangeIntensity);
        }
      }
      strBuilder.append(FORMAT_5_DIGITS.format(lowMassRangeIntensity/highMassRangeIntensity));
      writer.write(strBuilder.toString());
      writer.newLine();
      writer.flush();

    }

    writer.flush();
    writer.close();
  }


}
