package fr.profi.mzknife.mzdb;

import fr.profi.mzknife.CommandArguments;

public enum MgfBoostConfigTemplate {

  DISCOVERY( false,true ,2 , 0.0f , CommandArguments.ScanSelectorMode.ALL , 0.125 , 2),
  HISTONE( true,true ,1 , 0.2f , CommandArguments.ScanSelectorMode.SAME_CYCLE , 0.125 , 5),
  XLINK( false,true ,2 , 0.0f , CommandArguments.ScanSelectorMode.SAME_CYCLE, 0.125 , 2);

  final boolean useHeader;
  final boolean useSelectionWindow;
  final int swMaxPrecursorsCount;
  final float swIntensityThreshold;
  final CommandArguments.ScanSelectorMode scanSelector;
  final double pifThreshold;
  final int rankThreshold;


  MgfBoostConfigTemplate(boolean useHeader, boolean useSelectionWindow, int swMaxPrecursorsCount, float swIntensityThreshold, CommandArguments.ScanSelectorMode scanSelector, double pifThreshold, int rankThreshold) {
    this.useHeader = useHeader;
    this.useSelectionWindow = useSelectionWindow;
    this.swMaxPrecursorsCount = swMaxPrecursorsCount;
    this.swIntensityThreshold = swIntensityThreshold;
    this.scanSelector = scanSelector;
    this.pifThreshold = pifThreshold;
    this.rankThreshold = rankThreshold;
  }

  public boolean isUseHeader() {
    return useHeader;
  }

  public boolean isUseSelectionWindow() {
    return useSelectionWindow;
  }

  public int getSwMaxPrecursorsCount() {
    return swMaxPrecursorsCount;
  }

  public float getSwIntensityThreshold() {
    return swIntensityThreshold;
  }

  public CommandArguments.ScanSelectorMode getScanSelector() {
    return scanSelector;
  }

  public double getPifThreshold() {
    return pifThreshold;
  }

  public int getRankThreshold() {
    return rankThreshold;
  }
}
