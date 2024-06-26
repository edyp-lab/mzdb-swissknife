package fr.profi.mzknife.mgf;

import Preprocessing.Config;
import Preprocessing.JPeak;
import Preprocessing.JSpectrum;
import fr.profi.mzdb.io.writer.mgf.ISpectrumProcessor;
import fr.profi.mzdb.io.writer.mgf.MgfPrecursor;
import fr.profi.mzdb.model.SpectrumData;
import fr.profi.mzscope.InvalidMGFFormatException;
import fr.profi.mzscope.MSMSSpectrum;
import fr.profi.util.ms.package$;

import java.io.File;
import java.util.Optional;

public class PCleanProcessor extends MGFRewriter implements ISpectrumProcessor {

  public static final double MS2_DEFAULT_TOL = 0.05;

  Optional<String> labelMethodName = Optional.empty();
  Boolean imonFilter;
  Boolean repFilter;
  Boolean labelFilter;
  Boolean lowWinFilter;
  Boolean highWinFilter;
  Boolean isoReduction;
  Boolean chargeDeconv;
  Boolean ionsMerge;
  Boolean largerThanPrecursor;

  public PCleanProcessor(String method) {
    super();
    setLabelMethod(method);
    JSpectrum.setImmoniumIons();
    setPCleanParameters(true, true, true, false, false, true, true, false, true);
  }

  @Override
  public String getMethodName() {
    return "pClean";
  }

  @Override
  public String getMethodVersion() {
    return "1.0";
  }

  private void setLabelMethod(String method) {
    if ((method != null) && !method.isEmpty()) {
      labelMethodName = Optional.of(method);
    }
  }

  public PCleanProcessor(File srcFile, File m_dstFile, String method) throws InvalidMGFFormatException {
    super(srcFile, m_dstFile);
    setLabelMethod(method);
    JSpectrum.setImmoniumIons();
  }

  public void setPCleanParameters(Boolean imonFilter, Boolean repFilter, Boolean labelFilter, Boolean lowWinFilter, Boolean highWinFilter, Boolean isoReduction, Boolean chargeDeconv, Boolean ionsMerge, Boolean largerThanPrecursor) {
    this.imonFilter = imonFilter;
    this.repFilter = repFilter;
    this.labelFilter = labelFilter;
    this.lowWinFilter = lowWinFilter;
    this.highWinFilter = highWinFilter;
    this.isoReduction = isoReduction;
    this.chargeDeconv = chargeDeconv;
    this.ionsMerge = ionsMerge;
    this.largerThanPrecursor = largerThanPrecursor;
  }

  public void setPCleanParameters(PCleanConfigTemplate template) {
    setPCleanParameters(template.getImonFilter(), template.getRepFilter(), template.getLabelFilter(), template.getLowWinFilter(), template.getHighWinFilter(), template.getIsoReduction(), template.getChargeDeconv(), template.getIonsMerge(), template.getLargerThanPrecursor());
  }

  protected MSMSSpectrum getSpectrum2Export(MSMSSpectrum inSpectrum){

    JSpectrum jSpectrum = new JSpectrum();

    jSpectrum.setParentMass(package$.MODULE$.mozToMass(inSpectrum.getPrecursorMz(), inSpectrum.getPrecursorCharge()));
    jSpectrum.setParentMassToCharge(inSpectrum.getPrecursorMz());
    jSpectrum.setCharge(inSpectrum.getPrecursorCharge());
    jSpectrum.setIntensity(inSpectrum.getPrecursorIntensity());
    jSpectrum.setRt(inSpectrum.getRetentionTime());

    double[] mzList = inSpectrum.getMassValues();
    double[] intensList = inSpectrum.getIntensityValues();

    for (int i = 0; i< inSpectrum.peaksCount(); i++) {
      JPeak jPeak = new JPeak(mzList[i], intensList[i]);
      jSpectrum.addRawPeak(jPeak);
    }

    doModule12(jSpectrum);

    double[] masses = new double[jSpectrum.getPeaks().size()];
    double[] intensities = new double[jSpectrum.getPeaks().size()];
    int k = 0;
    for (JPeak p : jSpectrum.getPeaks()) {
      masses[k] = p.getMz();
      intensities[k++] = p.getIntensity();
    }

    MSMSSpectrum outSpectrum = new MSMSSpectrum(
            inSpectrum.getPrecursorMz(),
            inSpectrum.getPrecursorIntensity(),
            inSpectrum.getPrecursorCharge(),
            inSpectrum.getRetentionTime());

    inSpectrum.getAnnotations().forEachRemaining(a -> outSpectrum.setAnnotation(a, inSpectrum.getAnnotation(a)));

    for(int i = 0; i < masses.length; i++) {
      outSpectrum.addPeak(masses[i], intensities[i]);
    }

    return outSpectrum;
  }

  @Override
  public SpectrumData processSpectrum(MgfPrecursor mgfPrecursor, SpectrumData spectrumData) {

    JSpectrum jSpectrum = new JSpectrum();

    Config.ms2tol = MS2_DEFAULT_TOL;
    jSpectrum.setParentMass(package$.MODULE$.mozToMass(mgfPrecursor.getPrecMz(), mgfPrecursor.getCharge()));
    jSpectrum.setParentMassToCharge(mgfPrecursor.getPrecMz());
    jSpectrum.setCharge(mgfPrecursor.getCharge());
    //    jSpectrum.setSpectrumTitle(spectrum.getSpectrumTitle)
    jSpectrum.setIntensity(0.0);
    //    jSpectrum.setRt(spectrum.getPrecursor.getRt)

    double[] mzList = spectrumData.getMzList();
    float[] intensList = spectrumData.getIntensityList();

    for (int i = 0; i< spectrumData.getPeaksCount(); i++) {
      JPeak jPeak = new JPeak(mzList[i], intensList[i]);
      jSpectrum.addRawPeak(jPeak);
    }

    doModule12(jSpectrum);

    double[] newMzList = new double[jSpectrum.getPeaks().size()];
    float[] newIntensityList = new float[jSpectrum.getPeaks().size()];
    int k = 0;
    for (JPeak p : jSpectrum.getPeaks()) {
      newMzList[k] = p.getMz();
      newIntensityList[k++] = (float)p.getIntensity();
    }

    SpectrumData newSpectrumData = new SpectrumData(newMzList, newIntensityList);

    return newSpectrumData;
  }

  private void doModule12(JSpectrum jSpectrum) {

    jSpectrum.resetPeaks();

    if (labelMethodName.isPresent() && !repFilter) {
      JSpectrum.IsobaricTag tag = JSpectrum.IsobaricTag.valueOf(labelMethodName.get().toUpperCase());
      jSpectrum.detectReporterPeaks(tag.reporterIons, 0.01);
    }

    if (imonFilter) {
      jSpectrum.removeImmoniumIons();
    }

    /*module1 treatment*/
    if (labelMethodName.isPresent()) {
      jSpectrum.sortPeaksByMZ();
      jSpectrum.module1(labelMethodName.get(), repFilter, labelFilter, lowWinFilter, highWinFilter);
    }
    /*module2 treatment*/
    jSpectrum.sortPeaksByMZ();
    jSpectrum.module2(isoReduction, chargeDeconv, ionsMerge, largerThanPrecursor);

    if (labelMethodName.isPresent() && !repFilter) {
      jSpectrum.restoreReporterPeaks();
    }
  }

  }
