package fr.profi.mzknife.recalibration;

import com.beust.jcommander.Parameter;

public class RecalibrateArguments {


  @Parameter(names = {"-i","--input"}, description = "mzdb input file to recalibrate", required = true, order = 0)
  String inputFileName;

  @Parameter(names = {"-o","--output"}, description = "mzdb recalibrated output file", required = false, order = 1)
  String outputFileName;

  @Parameter(names = {"-d","--delta_mass"}, description = "the delta mass (in ppm) that must be added to the masses", required = true, order = 2)
  Double deltaMass;

  @Parameter(names = {"-fs","--first_scan"}, description = "the first scan where the recalibration will be applied", required = false, order = 3)
  Long firstScan = Long.MIN_VALUE;

  @Parameter(names = {"-ls","--last_scan"}, description = "the last scan where the recalibration is applied", required = false, order = 4)
  Long lastScan = Long.MAX_VALUE;

  @Parameter(names = {"-ft","--first_time"}, description = "the retention time (in seconds) from which the recalibration will be applied", required = false, order = 5)
  Double firstTime = Double.MIN_VALUE;

  @Parameter(names = {"-lt","--last_time"}, description = "the retention time (in seconds) at which the recalibration is applied", required = false, order = 6)
  Double lastTime = Double.MAX_VALUE;

  @Parameter(names = "--help", help = true, order = 7)
  boolean help;
}
