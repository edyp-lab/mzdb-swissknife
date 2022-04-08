package fr.profi.mzknife;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.util.List;

public class RewriterArguments {

  public final static String MZDB_RECAL_COMMAND_NAME = "recalibate_mzdb";
  public final static String MGF_RECAL_COMMAND_NAME = "recalibate_mgf";
  public final static String MGF_FILTER_COMMAND_NAME = "filter_mgf";
  public final static String MZDB_SPLIT_COMMAND_NAME = "split_mzdb";

  @Parameters(commandNames =  {MZDB_RECAL_COMMAND_NAME}, commandDescription = "Recalibrate mzDB file using delta mass. Recalibration will be applied only on specified scans range.", separators = "=")
  public static class MzDBRecalibrateCommand {

    @Parameter(names = {"-i","--input"}, description = "mzdb input file to recalibrate", required = true, order = 0)
    String inputFileName;

    @Parameter(names = {"-o","--output"}, description = "mzdb recalibrated output file", required = false, order = 1)
    String outputFileName;

    @Parameter(names = {"-d","--delta_mass"}, description = "the delta mass (in ppm) that must be added to the masses", required = true)
    Double deltaMass;

    @Parameter(names = {"-fs","--first_scan"}, description = "the first scan where the recalibration will be applied", required = true)
    Long firstScan = Long.MIN_VALUE;

    @Parameter(names = {"-ls","--last_scan"}, description = "the last scan where the recalibration is applied", required = true)
    Long lastScan = Long.MAX_VALUE;

    @Parameter(names = "--help", help = true)
    boolean help;

  }

  @Parameters(commandNames =  {MGF_RECAL_COMMAND_NAME}, commandDescription = "Recalibrate mgf file using delta mass. Recalibration will be applied only on specified time range.", separators = "=")
  public static class MgfRecalibrateCommand {

    @Parameter(names = {"-i","--input"}, description = "mgf input file to recalibrate", required = true, order = 0)
    String inputFileName;

    @Parameter(names = {"-o","--output"}, description = "mgf recalibrated output file", required = false, order = 1)
    String outputFileName;

    @Parameter(names = {"-d","--delta_mass"}, description = "the delta mass (in ppm) that must be added to the masses", required = true)
    Double deltaMass;


    @Parameter(names = {"-ft","--first_time"}, description = "the retention time (in seconds) from which the recalibration will be applied", required = true)
    Double firstTime = Double.MIN_VALUE;

    @Parameter(names = {"-lt","--last_time"}, description = "the retention time (in seconds) at which the recalibration is applied", required = true)
    Double lastTime = Double.MAX_VALUE;

    @Parameter(names = "--help", help = true)
    boolean help;

  }

  @Parameters(commandNames =  {MGF_FILTER_COMMAND_NAME}, commandDescription = "Filter mgf file using specified parameters.", separators = "=")
  public static class MgfFilterCommand {

    @Parameter(names = {"-i","--input"}, description = "mgf input file to filter", required = true, order = 0)
    String inputFileName;

    @Parameter(names = {"-o","--output"}, description = "Filtered mgf output file", required = false, order = 1)
    String outputFileName;

    @Parameter(names = {"-z","--charges"}, description = "Keep Spectrum WITH specified charges (list with space as separator). If specified, \"exclude-charges\" should NOT be specified.", required = false,  variableArity = true)
    List<Integer> charges2Keep;

    @Parameter(names = {"-nz","--exclude-charges"}, description = "Keep Spectrum WITHOUT specified charges (mist with space as separator). If specified, \"charges\" should NOT be specified. ", required = false,  variableArity = true)
    List<Integer> charges2Ignore;

    @Parameter(names = "--help", help = true)
    boolean help;

  }

  @Parameters(commandNames =  {MZDB_SPLIT_COMMAND_NAME}, commandDescription = "Split Exploris mzdb in as many files as existing CV; using specified parameters.", separators = "=")
  public static class MzDBSplitterCommand {

    @Parameter(names = {"-i","--input"}, description = "mzdb input file to recalibrate", required = true, order = 0)
    String inputFileName;

    @Parameter(names = "--help", help = true)
    boolean help;

  }
}
