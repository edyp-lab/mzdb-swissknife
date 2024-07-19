package fr.profi.mzknife;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fr.profi.mzknife.mgf.ECleanConfigTemplate;
import fr.profi.mzknife.mgf.PCleanConfigTemplate;
import fr.profi.mzknife.mgf.PCleanProcessor;

import java.util.List;

public class CommandArguments {

  public final static String RECALIBRATE_COMMAND_NAME = "recalibrate";
  public final static String SPLIT_COMMAND_NAME = "split";
  public final static String FILTER_COMMAND_NAME = "filter";
  public final static String CLEAN_COMMAND_NAME = "eclean";
  public final static String MERGE_COMMAND_NAME = "merge";

  public final static String MGF_METRICS_COMMAND_NAME = "metrics";

  public final static String MZDB_METRICS_COMMAND_NAME = "metrics";
  public final static String CREATE_MGF_COMMAND_NAME = "create_mgf";

  public final static String PCLEAN_COMMAND_NAME = "pclean";

  public final static String MATCH_IONS_COMMAND_NAME = "match_ions";
  public final static String MATCH_PSMS_COMMAND_NAME = "match_psms";


  @Parameters(commandNames =  {RECALIBRATE_COMMAND_NAME}, commandDescription = "Recalibrate mzDB file using delta mass. Recalibration will be applied only on specified scans range.", separators = "=")
  public static class MzDBRecalibrateCommand {

    @Parameter(names = {"-i","--input"}, description = "mzdb input file to recalibrate", required = true, order = 0)
    public String inputFileName;

    @Parameter(names = {"-o","--output"}, description = "mzdb recalibrated output file", required = false, order = 1)
    public String outputFileName;

    @Parameter(names = {"-d","--delta_mass"}, description = "the delta mass (in ppm) that must be added to the masses", required = true)
    public Double deltaMass;

    @Parameter(names = {"-fs","--first_scan"}, description = "the first scan where the recalibration will be applied", required = true)
    public Long firstScan = Long.MIN_VALUE;

    @Parameter(names = {"-ls","--last_scan"}, description = "the last scan where the recalibration is applied", required = true)
    public Long lastScan = Long.MAX_VALUE;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;

  }

  @Parameters(commandNames =  {RECALIBRATE_COMMAND_NAME}, commandDescription = "Recalibrate MGF file using delta mass. Recalibration (precursor and fragments) will be applied only on specified time range.", separators = "=")
  public static class MgfRecalibrateCommand {

    @Parameter(names = {"-i","--input"}, description = "mgf input file to recalibrate", required = true, order = 0)
    public String inputFileName;

    @Parameter(names = {"-o","--output"}, description = "mgf recalibrated output file", required = false, order = 1)
    public String outputFileName;

    @Parameter(names = {"-d","--delta_mass"}, description = "the delta mass (in ppm) that must be added to the masses", required = true)
    public Double deltaMass;


    @Parameter(names = {"-ft","--first_time"}, description = "the retention time (in seconds) from which the recalibration will be applied", required = true)
    public Double firstTime = Double.MIN_VALUE;

    @Parameter(names = {"-lt","--last_time"}, description = "the retention time (in seconds) at which the recalibration is applied", required = true)
    public Double lastTime = Double.MAX_VALUE;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;

  }

  @Parameters(commandNames =  {MZDB_METRICS_COMMAND_NAME}, commandDescription = "Compute some metrics from an mzDB input file.", separators = "=")
  public static class MzDBMetricsCommand {

    @Parameter(names = {"-i","--input"}, description = "the mzDB input file.", required = true, order = 0)
    public String inputFileName;

    @Parameter(names = {"-o","--output"}, description = "metrics output file", required = false, order = 1)
    public String outputFileName;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;

  }

  @Parameters(commandNames =  {FILTER_COMMAND_NAME}, commandDescription = "Filter MGF file using specified parameters.", separators = "=")
  public static class MgfFilterCommand {

    @Parameter(names = {"-i","--input"}, description = "mgf input file to filter", required = true, order = 0)
    public String inputFileName;

    @Parameter(names = {"-o","--output"}, description = "Filtered mgf output file", required = false, order = 1)
    public String outputFileName;

    @Parameter(names = {"-z","--charges"}, description = "Keep Spectrum WITH specified charges (list with space as separator). If specified, \"exclude-charges\" should NOT be specified.", required = false,  variableArity = true)
    public List<Integer> charges2Keep;

    @Parameter(names = {"-nz","--exclude-charges"}, description = "Keep Spectrum WITHOUT specified charges (mist with space as separator). If specified, \"charges\" should NOT be specified. ", required = false,  variableArity = true)
    public List<Integer> charges2Ignore;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;

  }


  @Parameters(commandNames =  {CLEAN_COMMAND_NAME}, commandDescription = "Clean (EDyP method) MS/MS fragment peaks of an MGF file.", separators = "=")
  public static class MgfCleanerCommand {

    @Parameter(names = {"-i","--input"}, description = "mgf input file to clean", required = true, order = 0)
    public String inputFileName;

    @Parameter(names = {"-mztol", "--mz_tol_ppm"}, description = "m/z tolerance used to detect fragment peaks.", required = false, order = 1)
    public Float mzTolPPM = 20.0f;

    @Parameter(names = {"-lm", "--label_method"}, description = "clean fragments associated with the selected method (ITRAQ4PLEX, ITRAQ8PLEX, TMT6PLEX, TMT10PLEX, TMT11PLEX, TMT16PLEX, TMT18PLEX).", required = false)
    public String labelingMethodName = null;

    @Parameter(names = {"-o","--output"}, description = "mgf output file", required = false, order = 2)
    public String outputFileName;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;

  }


  @Parameters(commandNames =  {MERGE_COMMAND_NAME}, commandDescription = "Merge 2 MGF files by setting fragments from the second file to precursors from the first file. The matching is based on spectrum scan numbers.", separators = "=")
  public static class MgfMergerCommand {

    @Parameter(names = {"-i1","--input1"}, description = "the mgf input file to be merged", required = true, order = 0)
    public String inputFileName1;

    @Parameter(names = {"-i2","--input2"}, description = "the mgf input file used to filter/merge the first input file", required = true, order = 1)
    public String inputFileName2;

    @Parameter(names = {"-f","--filter"}, description = "filter entries from mgf input file (if no corresponding scan in the second file, remove the entry)", required = false, arity = 1)
    public boolean filter = true;

    @Parameter(names = {"-r","--replace"}, description = "replace fragments of the first mgf file by fragments from the second one", required = false, arity = 1)
    public boolean replace = true;

    @Parameter(names = {"-o","--output"}, description = "mgf output file", required = false, order = 3)
    public String outputFileName;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;

  }

  @Parameters(commandNames =  {MGF_METRICS_COMMAND_NAME}, commandDescription = "Compute some metrics from an MGF input file.", separators = "=")
  public static class MgfMetricsCommand {

    @Parameter(names = {"-i","--input"}, description = "the MGF input file.", required = true, order = 0)
    public String inputFileName;

    @Parameter(names = {"-o","--output"}, description = "metrics output file", required = false, order = 1)
    public String outputFileName;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;

  }

  @Parameters(commandNames =  {CREATE_MGF_COMMAND_NAME}, commandDescription = "Creates an MGF file from a mzDB file. Options preceded by [mgf_boost] are only available " +
          "if the precursor_mz method is mgf_boost. ", separators = "=")
  public static class MzDBCreateMgfCommand {

    @Parameter(names = {"-mzdb", "--mzdb_file_path"}, description = "mzDB file to perform peaklist extraction", required = true)
    public String mzdbFile;

    @Parameter(names = {"-o", "--output_file_path"}, description = "MGF output file path", required = true)
    public String outputFile = "";

    @Parameter(names = {"-ms", "--ms_level"}, description = "the MS level to export", required = false)
    public Integer msLevel= 2;

    @Parameter(names = {"-precmz", "--precursor_mz"}, description = "the precursor computation method used : 'main_precursor_mz, selected_ion_mz, refined, refined_thermo, isolation_window_extracted, mgf_boost'.", required = false)
    public String precMzComputation = "main_precursor_mz";

    @Parameter(names = {"-mztol", "--mz_tol_ppm"}, description = "m/z tolerance used for precursor m/z value definition.", required = false)
    public Float mzTolPPM = 10.0f;

    @Parameter(names = {"-cutoff", "--intensity_cutoff"}, description = "optional intensity cutoff.", required = false)
    public Float intensityCutoff = 0.0f;

    @Parameter(names = {"-ptitle", "--proline_title"}, description = "export spectrum 'TITLE' line using the Proline convention.", required = false)
    public Boolean exportProlineTitle= false;

    @Parameter(names = {"-cMethod", "--clean_method"}, description = "Clean the generated MS2 spectra using the specified method (None, pClean or eClean).", required = false)
    public String cleanMethod = "None";

    @Parameter(names = {"-cLabelMethod", "--clean_label_method"}, description = "Apply clean Label filtering (pre-configured module1) associated with the selected method (ITRAQ4PLEX, ITRAQ8PLEX, TMT6PLEX, TMT10PLEX, TMT11PLEX, TMT16PLEX, TMT18PLEX).", required = false)
    public String cleanLabelMethodName = "";

    @Parameter(names = {"-cConfig" , "--clean_config_template"}, description = "Clean config template to use. Mandatory if a clean method is specified. Use one of (LabelFree, XLink or TMTLabelling) values.", converter = CleanConfigConverter.class, required = false)
    public CleanConfig cleanConfig;

    @Parameter(names = {"-da", "--dump_annotations"}, description = "Dump precursor computer annotation's in a separate dump file for statistical analyses purposes.", required = false)
    public Boolean dAnnot = false;

    @Parameter(names = {"-header", "--ms2_header"}, description = "[mgf_boost] Allow precursor detection from the MS2 header.", arity = 1, required = false)
    public Boolean useHeader = true;

    @Parameter(names = {"-sw", "--selection_window"}, description = "[mgf_boost] Allow precursors detection from selection window content", arity = 1, required = false)
    public Boolean useSelectionWindow = true;

    @Parameter(names = {"-sw_m", "--sw_max_precursors"}, description = "[mgf_boost] Maximum number of precursors extracted from the selection window if selection_window option is true.", required = false)
    public Integer swMaxPrecursorsCount = 1;

    @Parameter(names = {"-sw_t", "--sw_threshold"}, description = "[mgf_boost] Intensity threshold to consider peaks from the selection window (percentage of the max peak of the selection window).", required = false)
    public Float swIntensityThreshold = 0.2f;

    @Parameter(names = {"-pif_t", "--pif_threshold"}, description = "[mgf_boost] Minimum PIF (Precursor Ion Fraction) value used to post-filter generated precursors (only precursors with a PIF value larger than or equal to the threshold are retained)", required = false)
    public Double pifThreshold = 0.125;

    @Parameter(names = {"-rk_t", "--rank_threshold"}, description = "[mgf_boost] Maximum peak intensity rank value used to post-filter generated precursors (only precursors with a rank value less than or equal to the threshold are retained).", required = false)
    public Integer rankThreshold = 2;

    @Parameter(names = {"-mss", "--ms1_scan_selector"}, description = "[mgf_boost] MS1 scan selector mode. Controls the MS1 scan from which precursors are detected : MASTER_SCAN (use the MS1 scan referenced as the 'master scan' if available), SAME_CYCLE (use the first preceding MS1 scan), NEAREST (use the MS1 scan nearest to the MS2 event), ALL (use all scans)", required = false)
    public ScanSelectorMode scanSelectorMode = ScanSelectorMode.MASTER_SCAN;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;

  }

  @Parameters(commandNames =  {SPLIT_COMMAND_NAME}, commandDescription = "Split Exploris mzdb in as many files as existing CV; using specified parameters.", separators = "=")
  public static class MzDBSplitterCommand {

    @Parameter(names = {"-i","--input"}, description = "mzdb input file to recalibrate", required = true, order = 0)
    public String inputFileName;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;

  }

  @Parameters(commandNames = {CREATE_MGF_COMMAND_NAME}, commandDescription = "Converts MaxQuant generated .apl files into MGF file", separators = "=")
  public static class MaxQuantMGFCommand {

    @Parameter(names = {"-i1","--input1"}, description = "the MaxQuant multi apl file (.sil0.apl) ", required = true, order = 0)
    public String inputFileName1;

    @Parameter(names = {"-i2","--input2"}, description = "the MaxQuant peak apl file (.peak.apl) ", required = true, order = 1)
    public String inputFileName2;

    @Parameter(names = {"-o","--output"}, description = "the MGF output file", required = true, order = 2)
    public String outputFileName;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;

  }

  @Parameters(commandNames = {PCLEAN_COMMAND_NAME}, commandDescription = "Clean (pClean method) to MS/MS fragment peaks of an MGF file", separators = "=")
  public static class PCleanCommand {

    @Parameter(names = {"-mgf"}, description = "Input MS/MS data", required = true)
    public String mgf;
    @Parameter(names = {"-o","--output"}, description = "the MGF output file", required = true, order = 2)
    public String outputFileName;
    @Parameter(names = {"-itol"}, description = "Fragment ion tolerance (Da)", required = false)
    public Double itol = PCleanProcessor.MS2_DEFAULT_TOL;
    @Parameter(names = {"-aa2"}, description = "Consider mass gap of two amino acids", required = false)
    public Boolean aa2 = true;
    @Parameter(names = {"-mionFilter"}, description = "Filter out immonium ions", required = false, arity = 1)
    public Boolean ionFilter = true;
    @Parameter(names = {"-labelMethod"}, description = "Peptide labeling method (ITRAQ4PLEX, ITRAQ8PLEX, TMT6PLEX, TMT10PLEX, TMT11PLEX, TMT16PLEX, TMT18PLEX)", required = false)
    public String labelMethod = null;
    @Parameter(names = {"-repFilter"}, description = "Filter out reporter ions (only used if labelMethod is defined)", required = false, arity = 1)
    public Boolean repFilter = true;
    @Parameter(names = {"-labelFilter"}, description = "Filter out label-associated ions (only used if labelMethod is defined)", required = false, arity = 1)
    public Boolean labelFilter = true;
    @Parameter(names = {"-low"}, description = "Clearance of low b-/y-ion free window", required = false, arity = 1)
    public Boolean low = false;
    @Parameter(names = {"-high"}, description = "Clearance of high b-/y-ion free window", required = false, arity = 1)
    public Boolean high = false;
    @Parameter(names = {"-isoReduction"}, description = "Heavy isotopic ions reduction", required = false, arity = 1)
    public Boolean isoReduction = true;
    @Parameter(names = {"-chargeDeconv"}, description = "High charge deconvolution", required = false, arity = 1)
    public Boolean chargeDeconv = true;
    @Parameter(names = {"-largerThanPrecursor"}, description = "Filter out ions larger than precursor’s mass", required = false, arity = 1)
    public Boolean largerThanPrecursor = true;
    @Parameter(names = {"-ionsMerge"}, description = "Merge two ions of similar mass", required = false, arity = 1)
    public Boolean ionsMerge = false;
    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;
  }

  public enum CleanConfig {
    LABEL_FREE("LabelFree","Label Free", PCleanConfigTemplate.LABEL_FREE_CONFIG, ECleanConfigTemplate.LABEL_FREE_CONFIG),
    XLINK("XLink","XLink", PCleanConfigTemplate.XLINK_CONFIG, ECleanConfigTemplate.XLINK_CONFIG),
    TMT_LABELED("TMTLabelling", "TMT Labelling", PCleanConfigTemplate.TMT_LABELLING_CONFIG, ECleanConfigTemplate.TMT_LABELLING_CONFIG);

    final String commandValue;
    final String displayValue;
    final PCleanConfigTemplate pCleanConfigTemplate;
    final ECleanConfigTemplate eCleanConfigTemplate;

    CleanConfig(String cmdVal, String displayVal, PCleanConfigTemplate pCleanConfigTemplate, ECleanConfigTemplate eCleanConfigTemplate) {
      this.commandValue = cmdVal;
      this.displayValue = displayVal;
      this.pCleanConfigTemplate = pCleanConfigTemplate;
      this.eCleanConfigTemplate = eCleanConfigTemplate;
    }

    public String getConfigCommandValue(){
      return commandValue;
    }

    public String getDisplayValue(){
      return displayValue;
    }


    public List<String>  stringifyPCleanParametersList() {
      return getPCleanConfigTemplate().stringifyParametersList();
    }

    public List<String>  stringifyECleanParametersList() {
      return getECleanConfigTemplate().stringifyParametersList();
    }

    public PCleanConfigTemplate getPCleanConfigTemplate(){
      return pCleanConfigTemplate;
    }

    public ECleanConfigTemplate getECleanConfigTemplate(){
      return eCleanConfigTemplate;
    }

    public static CleanConfig getConfigFromCommandValue(String cmdValue) {
      for (CleanConfig next : CleanConfig.values()) {
        if (next.commandValue.equals(cmdValue))
          return next;
      }
      return null;
    }

    public static CleanConfig getConfigFromDisplayValue(String value) {
      for (CleanConfig next : CleanConfig.values()) {
        if (next.displayValue.equals(value))
          return next;
      }
      return null;
    }

    @Override
    public String toString(){
      return getDisplayValue();
    }
  }

  public enum ScanSelectorMode {
    MASTER_SCAN, SAME_CYCLE, NEAREST, ALL
  }

  @Parameters(commandNames = {MATCH_IONS_COMMAND_NAME}, commandDescription = "Search putative ions in a peakeldb file", separators = "=")
  public static class IonsMatchingCommand {

    @Parameter(names = {"-pkdb","--peakeldb_file"}, description = "peakeldb input file ", required = true, order = 0)
    public String peakelDbFile;

    @Parameter(names = {"-ions","--ions_file"}, description = "putative ions to search for in the peakeldb", required = true, order = 1)
    public String putativeIonsFile;

    @Parameter(names = {"-ftdb","--featuredb_file"}, description = "featuredb containing matched features and peakels", required = false, order = 2)
    public String featureDbFile;

    @Parameter(names = {"-o","--output"}, description = "matching peakels", required = false, order = 1)
    public String outputFile;

    @Parameter(names = {"-mztol", "--mz_tol_ppm"}, description = "m/z tolerance used for matching ions.", required = false)
    public Float mzTolPPM = 5.0f;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;
  }

  @Parameters(commandNames = {MATCH_PSMS_COMMAND_NAME}, commandDescription = "Match identified PSMs to peakels from peakeldb file", separators = "=")
  public static class PsmsMatchingCommand {

    @Parameter(names = {"-mzdb","--mzdbdb_file"}, description = "mzdb input file ", required = true, order = 0)
    public String mzDbFile;

    @Parameter(names = {"-pkdb","--peakeldb_file"}, description = "peakeldb input file ", required = true, order = 0)
    public String peakelDbFile;

    @Parameter(names = {"-psms","--psms_file"}, description = "identified PSMs to search for in the peakeldb", required = true, order = 1)
    public String psmsFile;

    @Parameter(names = {"-o","--output"}, description = "matching peakels", required = false, order = 1)
    public String outputFile;

    @Parameter(names = {"-mztol", "--mz_tol_ppm"}, description = "m/z tolerance used for matching ions.", required = false)
    public Float mzTolPPM = 5.0f;

    @Parameter(names = {"-h", "--help"}, help = true)
    public boolean help;
  }

}
