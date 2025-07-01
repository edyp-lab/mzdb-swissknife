package fr.profi.mzknife.mzdb;

import fr.profi.mzknife.CommandArguments;

public enum MgfBoostConfigTemplate {

  DISCOVERY( false,true , CommandArguments.ScanSelectorMode.ALL , 0.125 , 2),
  HISTONE( true,true , CommandArguments.ScanSelectorMode.SAME_CYCLE , 0.125 , 5),
  XLINK( false,true , CommandArguments.ScanSelectorMode.SAME_CYCLE, 0.125 , 2);

  final boolean useHeader;
  final boolean useSelectionWindow;
  final CommandArguments.ScanSelectorMode scanSelector;
  final double pifThreshold;
  final int takeThreshold;


  MgfBoostConfigTemplate(boolean useHeader, boolean useSelectionWindow, CommandArguments.ScanSelectorMode scanSelector, double pifThreshold, int takeThreshold) {
    this.useHeader = useHeader;
    this.useSelectionWindow = useSelectionWindow;
    this.scanSelector = scanSelector;
    this.pifThreshold = pifThreshold;
    this.takeThreshold = takeThreshold;
  }

  public boolean isUseHeader() {
    return useHeader;
  }

  public boolean isUseSelectionWindow() {
    return useSelectionWindow;
  }


  public CommandArguments.ScanSelectorMode getScanSelector() {
    return scanSelector;
  }

  public double getPifThreshold() {
    return pifThreshold;
  }

  public int getTakeThreshold() {
    return takeThreshold;
  }
}
