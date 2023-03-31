package fr.profi.mzknife;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fr.profi.mzknife.mgf.PCleanConfigTemplate;

import java.util.List;

public class CommandArguments {

  public final static String RECALIBRATE_COMMAND_NAME = "recalibrate";
  public final static String SPLIT_COMMAND_NAME = "split";
  public final static String FILTER_COMMAND_NAME = "filter";
  public final static String CLEAN_COMMAND_NAME = "clean";
  public final static String MERGE_COMMAND_NAME = "merge";
  public final static String CREATE_MGF_COMMAND_NAME = "create_mgf";

  public final static String PCLEAN_COMMAND_NAME = "pclean";

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

    @Parameter(names = "--help", help = true)
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

    @Parameter(names = "--help", help = true)
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

    @Parameter(names = "--help", help = true)
    public boolean help;

  }


  @Parameters(commandNames =  {CLEAN_COMMAND_NAME}, commandDescription = "Clean MS/MS fragment peaks of an MGF file.", separators = "=")
  public static class MgfCleanerCommand {

    @Parameter(names = {"-i","--input"}, description = "mgf input file to filter", required = true, order = 0)
    public String inputFileName;

    @Parameter(names = {"-o","--output"}, description = "mgf output file", required = false, order = 1)
    public String outputFileName;

    @Parameter(names = "--help", help = true)
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

    @Parameter(names = "--help", help = true)
    public boolean help;

  }


  @Parameters(commandNames =  {CREATE_MGF_COMMAND_NAME}, commandDescription = "Creates an MGF file from a mzDB file.", separators = "=")
  public static class MzDBCreateMgfCommand {

    @Parameter(names = {"-mzdb", "--mzdb_file_path"}, description = "mzDB file to perform extraction", required = true)
    public String mzdbFile;

    @Parameter(names = {"-o", "--output_file_path"}, description = "mgf output file path", required = true)
    public String outputFile = "";

    @Parameter(names = {"-ms", "--ms_level"}, description = "the MS level to export", required = false)
    public Integer msLevel= 2;

    @Parameter(names = {"-precmz", "--precursor_mz"}, description = "must be on of 'main_precursor_mz, selected_ion_mz, refined, refined_thermo, isolation_window_extracted, mgf_boost_v3.6'", required = false)
    public String precMzComputation = "main_precursor_mz";

    @Parameter(names = {"-mztol", "--mz_tol_ppm"}, description = "m/z tolerance used for precursor m/z value definition", required = false)
    public Float mzTolPPM = 20.0f;

    @Parameter(names = {"-cutoff", "--intensity_cutoff"}, description = "optional intensity cutoff to use", required = false)
    public Float intensityCutoff = 0.0f;

    @Parameter(names = {"-ptitle", "--proline_title"}, description = "export TITLE using the Proline convention", required = false)
    public Boolean exportProlineTitle= false;

    @Parameter(names = {"-pClean", "--pClean_ms2_processing"}, description = "Apply pClean to MS2 spectra (pre-configured module2)", required = false)
    public Boolean pClean = false;

    @Parameter(names = {"-pLabelMethod", "--pClean_label_method"}, description = "Apply pClean Label filtering (pre-configured module1) associated with the selected method (ITRAQ4PLEX, ITRAQ8PLEX, TMT6PLEX, TMT10PLEX, TMT11PLEX, TMT16PLEX, TMT18PLEX)", required = false)
    public String pCleanLabelMethodName = "";

    @Parameter(names = {"-pConfig" , "--pClean_config_template"}, description = "PClean config template to use. Mandatory if -pClean is specified (LabelFree, XLink or TMTLabelling)", converter = PCleanConfigConverter.class, required = false)
    public PCleanConfig pCleanConfig;

    @Parameter(names = "--help", help = true)
    public boolean help;

  }

  @Parameters(commandNames =  {SPLIT_COMMAND_NAME}, commandDescription = "Split Exploris mzdb in as many files as existing CV; using specified parameters.", separators = "=")
  public static class MzDBSplitterCommand {

    @Parameter(names = {"-i","--input"}, description = "mzdb input file to recalibrate", required = true, order = 0)
    public String inputFileName;

    @Parameter(names = "--help", help = true)
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

    @Parameter(names = "--help", help = true)
    public boolean help;

  }

  @Parameters(commandNames = {PCLEAN_COMMAND_NAME}, commandDescription = "Apply pClean to the supplied MGF file", separators = "=")
  public static class PCleanCommand {

    @Parameter(names = {"-mgf"}, description = "Input MS/MS data", required = true)
    public String mgf;
    @Parameter(names = {"-o","--output"}, description = "the MGF output file", required = true, order = 2)
    public String outputFileName;
    @Parameter(names = {"-itol"}, description = "Fragment ion tolerance (Da)", required = false)
    public Double itol = 0.05;
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
    @Parameter(names = "--help", help = true)
    public boolean help;
  }

  public enum PCleanConfig {
    LABEL_FREE("LabelFree","Label Free", PCleanConfigTemplate.LABEL_FREE_CONFIG),
    XLINK("XLink","XLink", PCleanConfigTemplate.XLINK_CONFIG),
    TMT_LABELED("TMTLabelling", "TMT Labelling", PCleanConfigTemplate.TMT_LABELLING_CONFIG);

    final String commandValue;
    final String displayValue;
    final PCleanConfigTemplate pCleanConfigTemplate;

    PCleanConfig(String cmdVal,String displayVal, PCleanConfigTemplate configTemplate) {
      commandValue = cmdVal;
      displayValue = displayVal;
      pCleanConfigTemplate= configTemplate;
    }

    public String getConfigCommandValue(){
      return commandValue;
    }

    public String getDisplayValue(){
      return displayValue;
    }

    public PCleanConfigTemplate getPCleanConfigTemplate(){
      return pCleanConfigTemplate;
    }

    public static PCleanConfig getConfigFor(String cmdValue) {
      for (PCleanConfig next : PCleanConfig.values()) {
        if (next.commandValue.equals(cmdValue))
          return next;
      }
      return null;
    }

    @Override
    public String toString(){
      return getDisplayValue();
    }
  }

}
