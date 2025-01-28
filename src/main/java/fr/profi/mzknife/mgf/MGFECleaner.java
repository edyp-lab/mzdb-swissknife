package fr.profi.mzknife.mgf;

import fr.profi.chemistry.model.BiomoleculeAtomTable$;
import fr.profi.chemistry.model.MolecularConstants;
import fr.profi.ms.model.MSMSSpectrum;
import fr.profi.ms.model.TheoreticalIsotopePattern;
import fr.profi.mzdb.algo.DotProductPatternScorer;
import fr.profi.mzdb.io.writer.mgf.ISpectrumProcessor;
import fr.profi.mzdb.io.writer.mgf.MgfPrecursor;
import fr.profi.mzdb.model.SpectrumData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class MGFECleaner extends MGFThreadedRewriter implements ISpectrumProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(MGFECleaner.class);

  private static final BiomoleculeAtomTable$ atomTable = BiomoleculeAtomTable$.MODULE$;

  private final IsobaricTag isobaricTags;
  private final double mzTolerancePpm;

  public static class Peak {

    final double mass;
    final double intensity;
    final int index;
    boolean used = false;

    public Peak(Peak peak) {
      this(peak.mass, peak.intensity, peak.index);
    }

    public Peak(double mass, double intensity, int index) {
      this.mass = mass;
      this.intensity = intensity;
      this.index = index;
    }
  }


  public enum IsobaricTag {
    ITRAQ4PLEX( 144.102063, new double[]{ 114.110679, 115.107714, 116.111069, 117.114424}),
    ITRAQ8PLEX( 304.205360, new double[]{ 113.107324, 114.110679, 115.107714, 116.111069, 117.114424, 118.111459, 119.114814, 121.121523}),
    TMT6PLEX(229.162932, new double[]{ 126.1277261, 127.1310809, 128.1344357, 129.1377905, 130.1411453, 131.1381802}),
    TMT10PLEX(229.162932, new double[]{ 126.1277261, 127.1247610 ,127.1310809, 128.1281158, 128.1344357, 129.1314706, 129.1377905, 130.1348254, 130.1411453, 131.1381802}),
    TMT11PLEX(229.162932, new double[]{ 126.1277261, 127.1247610 ,127.1310809, 128.1281158, 128.1344357, 129.1314706, 129.1377905, 130.1348254, 130.1411453, 131.1381802, 131.144999 }),
    TMT16PLEX( 304.207146 , new double[]{ 126.127726, 127.124761, 127.131081, 128.128116, 128.134436, 129.131471, 129.137790, 130.134825, 130.141145, 131.138180, 131.144500, 132.141535, 132.147855, 133.144890, 133.151210, 134.148245}),
    TMT18PLEX( 304.207146 , new double[]{ 126.127726, 127.124761, 127.131081, 128.128116, 128.134436, 129.131471, 129.137790, 130.134825, 130.141145, 131.138180, 131.144500, 132.141535, 132.147855, 133.144890, 133.151210, 134.148245, 134.154565, 135.151600}),
    TMT16PLEX_DEUTERATED( 304.2135 , new double[]{ 127.134003, 128.131038, 128.137358, 129.134393 ,129.140713, 130.137748, 130.144068, 131.141103, 131.147423, 132.144458, 132.150778, 133.147813, 133.154133, 134.151171, 134.157491, 135.154526}),
    TMT35PLEX( 304.2135 , new double[]{ 126.127726, 127.124761, 127.131081, 127.134003, 128.128116, 128.131038, 128.134436, 128.137358, 129.131471, 129.134393, 129.13779, 129.140713, 130.134825, 130.137748, 130.141145, 130.144068, 131.13818, 131.141103, 131.1445, 131.147423, 132.141535, 132.144458, 132.147855, 132.150778, 133.14489, 133.147813, 133.15121, 133.154133, 134.148245, 134.151171, 134.154565, 134.157491, 135.1516, 135.154526, 135.160846});
    public final double[] reporterIons;
    public final double tagMass;

    IsobaricTag(double mass, double[] reporters) {
      this.reporterIons = reporters;
      this.tagMass = mass;
    }

  }

  // https://www.gpmaw.com/tables/ResMassVal.pdf
  // https://www.biosyn.com/tew/amino-acid-masses-tables.aspx
  //
  public enum ImmoniumIon {

    GLYCINE(30.03438),
    ALANINE(44.05003),
    SERINE(60.04494),
    PROLINE(70.06568),
    VALINE(72.08133),
    THREONINE(74.06059),
    CYSTEINE(76.0221),
    CARBAMIDOMETHYLATED(133.0436),
    CARBOXYMETHYLATED(134.0276),
    ACRYLAMIDE_ADDUCT(147.0772),
    LEUCINE_ISOLEUCINE(86.09698),
    ASPARAGINE(87.05584),
    ASPARAGINE_A1(70.03),
    ASPARTIC_ACID(88.03986),
    GLUTAMINE(101.0715),
    GLUTAMINE_A1(129.10),
    GLUTAMINE_A2(84.04),
    GLUTAMINE_A3(56.05),
    LYSINE(101.1079),
    LYSINE_2(84.08136),
    GLUTAMIC_ACID(102.0555),
    METHIONINE(104.0534),
    OXIDIZED_METHIONINE(120.0483),
    HISTIDINE(110.0718),
    PHENYLALANINE(120.0813),
    ARGININE(129.114),
    ARGININE_A1(115.09),
    ARGININE_A2(112.09),
    ARGININE_A3(87.09),
    ARGININE_A4(60.06),
    TYROSINE(136.0762),
    TRYPTOPHAN(159.0922),
    TRYPTOPHAN_A1(132.08),
    TRYPTOPHAN_A2(130.07);
    private final double mass;

    ImmoniumIon(double mass) {
      this.mass = mass;
    }
  }

  protected MGFECleaner(double mzTolPpm) {
    mzTolerancePpm = mzTolPpm;
    isobaricTags = null;
  }

  public MGFECleaner(double mzTolerancePpm, String labelingMethodName) {
    this.mzTolerancePpm = mzTolerancePpm;
    if ((labelingMethodName != null) && !labelingMethodName.isEmpty()) {
      isobaricTags = IsobaricTag.valueOf(labelingMethodName.toUpperCase());
    } else {
      isobaricTags = null;
    }
  }

  public MGFECleaner(File srcFile, File m_dstFile, double mzTolPpm) throws IOException {
    super(srcFile, m_dstFile);
    mzTolerancePpm = mzTolPpm;
    isobaricTags = null;
  }

  public MGFECleaner(File srcFile, File m_dstFile, double mzTolPpm, IsobaricTag tags) throws IOException {
    super(srcFile, m_dstFile);
    mzTolerancePpm = mzTolPpm;
    this.isobaricTags = tags;
  }

  // Not yet used since ECleanConfigTemplate has no parameter for now.
  public void setECleanParameters(ECleanConfigTemplate eCleanConfigTemplate) {

  }

  @Override
  public String getMethodName() {
    return "edypClean";
  }

  @Override
  public String getMethodVersion() {
    return "1.1";
  }

  @Override
  public SpectrumData processSpectrum(MgfPrecursor mgfPrecursor, SpectrumData spectrumData) {

    int parentCharge = mgfPrecursor.getCharge();
    double parentMass = mgfPrecursor.getPrecMz()*parentCharge - parentCharge*MolecularConstants.PROTON_MASS();
    double[] masses = spectrumData.getMzList();
    float[] intensities = spectrumData.getIntensityList();

    List<Peak> peaks = new ArrayList<>(masses.length);
    Map<Integer, Peak> peaksByIndex = new HashMap<>();
    for (int k = 0; k < masses.length; k++) {
      Peak p = new Peak(masses[k], intensities[k], k);
      peaks.add(p);
      peaksByIndex.put(p.index, p);
    }

    List<Peak> result = processPeaks(parentMass, parentCharge, peaks, spectrumData,  peaksByIndex);

    masses = new double[result.size()];
    intensities = new float[result.size()];
    int k = 0;
    for (Peak p : result) {
      masses[k] = p.mass;
      intensities[k++] = (float)p.intensity;
    }

    SpectrumData newSpectrumData = new SpectrumData(masses, intensities);

    return newSpectrumData;
  }

  protected MSMSSpectrum getSpectrum2Export(MSMSSpectrum inSpectrum){

    int parentCharge = inSpectrum.getPrecursorCharge();
    double parentMass = inSpectrum.getPrecursorMz()*parentCharge - parentCharge*MolecularConstants.PROTON_MASS();
    double[] masses = inSpectrum.getMassValues();
    double[] intensities = inSpectrum.getIntensityValues();
    float[] fIntensities = new float[intensities.length];

    List<Peak> peaks = new ArrayList<>(masses.length);
    Map<Integer, Peak> peaksByIndex = new HashMap<>();
    for (int k = 0; k < masses.length; k++) {
      Peak p = new Peak(masses[k], intensities[k], k);
      peaks.add(p);
      peaksByIndex.put(p.index, p);
      fIntensities[k] = (float)intensities[k];
    }

    SpectrumData spectrumData = new SpectrumData(masses, fIntensities);


    List<Peak> result = processPeaks(parentMass, parentCharge, peaks, spectrumData,  peaksByIndex);


    masses = new double[result.size()];
    intensities = new double[result.size()];
    int k = 0;
    for (Peak p : result) {
      masses[k] = p.mass;
      intensities[k++] = p.intensity;
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

  private List<Peak> processPeaks(double parentMass, int parentCharge, List<Peak> peaks, SpectrumData spectrumData, Map<Integer, Peak> peaksByIndex) {

    List<Peak> result = new ArrayList<>(peaks.size());

    List<Double> immoniumIonMasses = Arrays.stream(ImmoniumIon.values()).map(i -> i.mass).sorted().toList();
    List<Double> reporterAssociatedIonMasses = new ArrayList<>(isobaricTags == null ? 0 : isobaricTags.reporterIons.length*3+3);

    if (isobaricTags != null) {
      for (double reporterMass : isobaricTags.reporterIons) {
        reporterAssociatedIonMasses.add(reporterMass);
        double reporterCOHMass = reporterMass + atomTable.getAtom("C").monoMass() + atomTable.getAtom("O").monoMass() + MolecularConstants.PROTON_MASS();
        reporterAssociatedIonMasses.add(reporterCOHMass);
        reporterAssociatedIonMasses.add(parentMass - reporterCOHMass);
      }

      reporterAssociatedIonMasses.add(isobaricTags.tagMass + MolecularConstants.PROTON_MASS());
      reporterAssociatedIonMasses.add(isobaricTags.tagMass + 2*MolecularConstants.PROTON_MASS());
      reporterAssociatedIonMasses.add(parentMass - isobaricTags.tagMass);

    }

    reporterAssociatedIonMasses.sort(Double::compareTo);

    peaks.sort((o1, o2) -> Double.compare(o2.intensity, o1.intensity));


    for (int k = 0; k < peaks.size(); k++) {

      Peak p = peaks.get(k);

      // Filter immonium Ions
      if (p.mass < 160) {
          for (double ionMass : immoniumIonMasses) {
            if (1e6*Math.abs(ionMass - p.mass)/p.mass < mzTolerancePpm) {
              p.used = true;
              break;
            } else if (ionMass > p.mass) {
              break;
            }
          }
      }

      // Filter reporter ions if isobaricTags is defined
      if (isobaricTags != null) {
          for (double ionMass : reporterAssociatedIonMasses) {
            if (1e6*Math.abs(ionMass - p.mass)/p.mass < mzTolerancePpm) {
              p.used = true;
              break;
            } else if (ionMass > p.mass) {
              break;
            }
          }
      }

      if (!p.used) {
        IsotopicPatternMatch patternMatch = predictIsotopicPattern(spectrumData, p.mass, mzTolerancePpm, parentCharge, peaksByIndex);
        if (patternMatch != null) {
          int charge = patternMatch.theoreticalPattern.charge();
          //LOG.info("pattern matching peak at ({},{}): ({},{}+)", p.mass, p.intensity, patternMatch.theoreticalPattern.monoMz(), charge);
          for(Optional<Integer> peakIndex : patternMatch.matchingPeaks) {
            if (!peakIndex.isEmpty()) {
              final Peak peak = peaksByIndex.get(peakIndex.get());
              peak.used = true;
            }
          }
          if (charge == 1) {
            final Integer peakIdx = patternMatch.matchingPeaks.get(0).get();
            result.add(new Peak(peaksByIndex.get(peakIdx)));
          } else {
            final Integer peakIdx = patternMatch.matchingPeaks.get(0).get();
            Peak monoPeak = peaksByIndex.get(peakIdx);
            Peak newPeak = new Peak(monoPeak.mass*charge - (charge-1)* MolecularConstants.PROTON_MASS(), monoPeak.intensity, p.index);
            //LOG.info("Move peak ({},{},{}+) to ({},{}, 1+)", monoPeak.mass, monoPeak.intensity, charge, newPeak.mass, newPeak.intensity);
            result.add(newPeak);
          }
        } else {
          p.used = true;
          result.add(new Peak(p));
        }
      }
    }

//    result.sort(Comparator.comparingDouble(o -> o.mass));

    result = result.stream().filter(p -> p.mass <= parentMass).sorted(Comparator.comparingDouble(o -> o.mass)).collect(Collectors.toList());
    return result;
  }

  public static int getPeakIndex(double[] peaksMz, double value, double ppmTol) {
    int idx = Arrays.binarySearch(peaksMz, value);
    idx = (idx < 0) ? ~idx : idx;
    double min = Double.MAX_VALUE;
    int resultIdx = -1;
    for (int k = Math.max(0, idx - 1); k <= Math.min(peaksMz.length - 1, idx + 1); k++) {
      if (((1e6 * Math.abs(peaksMz[k] - value) / value) < ppmTol) && (Math.abs(peaksMz[k] - value) < min)) {
        min = Math.abs(peaksMz[k] - value);
        resultIdx = k;
      }
    }
    return resultIdx;
  }

  public static IsotopicPatternMatch predictIsotopicPattern(SpectrumData spectrum, double mz, double ppmTol, int maxCharge, Map<Integer, Peak> peaksByIndex) {

    List<Tuple2<Object, TheoreticalIsotopePattern>> putativePatterns = new ArrayList<>();

    for (int charge = 1; charge <= maxCharge; charge++) {
      Tuple2<Object, TheoreticalIsotopePattern>[] patterns = DotProductPatternScorer.calcIsotopicPatternHypothesesFromCharge(spectrum, mz, charge, ppmTol);
      putativePatterns.addAll(Arrays.asList(patterns));
    }

    putativePatterns.sort((p1, p2) -> {
      double s1 =  ((Double)p1._1).doubleValue();
      double s2 =  ((Double)p2._1).doubleValue();
      return (s1 == s2) ? (p1._2.charge() - p2._2.charge()) : Double.compare(s1, s2);
    });

    List<IsotopicPatternMatch> matches = new ArrayList<>(putativePatterns.size());

    for(Tuple2<Object, TheoreticalIsotopePattern> prediction : putativePatterns) {
      IsotopicPatternMatch patternMatch = new IsotopicPatternMatch((Double)prediction._1, prediction._2, spectrum.getMzList(), ppmTol);
      matches.add(patternMatch);
    }

    matches = matches.stream().filter(m -> (m.score < 0.4) &&
            m.matchingPeaks.get(0).isPresent() && m.matchingPeaks.get(1).isPresent() &&
            !peaksByIndex.get(m.matchingPeaks.get(0).get()).used &&
            !peaksByIndex.get(m.matchingPeaks.get(1).get()).used ).collect(Collectors.toList());

    if (matches.size() > 1) {
      double maxScore = matches.get(0).score;
      matches = matches.stream().filter(m -> ((maxScore - m.score) < 0.1)).collect(Collectors.toList());
      if (matches.size() > 1) {
        int maxMatching = matches.stream().map(m -> m.matchingCount).max(Integer::compareTo).get();
        matches = matches.stream().filter(m -> m.matchingCount == maxMatching).collect(Collectors.toList());
      }
    }

    return matches.isEmpty() ? null : matches.get(0);
  }
}


class IsotopicPatternMatch {

  final double score;
  final TheoreticalIsotopePattern theoreticalPattern;
   int matchingCount;

  List<Optional<Integer>> matchingPeaks;

  public IsotopicPatternMatch(double score, TheoreticalIsotopePattern theoreticalPattern) {
    this.score = score;
    this.theoreticalPattern = theoreticalPattern;
  }

  public IsotopicPatternMatch(double score, TheoreticalIsotopePattern theoreticalPattern, List<Optional<Integer>> peaks) {
    this(score, theoreticalPattern);
    this.matchingPeaks = peaks;
    this.matchingCount = (int)peaks.stream().filter(p -> p.isPresent()).count();
  }

  public IsotopicPatternMatch(double score, TheoreticalIsotopePattern theoreticalPattern, double[] masses, double tolPPM) {
    this(score, theoreticalPattern);
    // limit to 3 isotopes max
    List<Optional<Integer>> peaks = new ArrayList<>(3);
    int count = 0;
    for (int k = 0; k < 3; k++) {
      Tuple2<Object, Object> p = theoreticalPattern.mzAbundancePairs()[k];
      int index = MGFECleaner.getPeakIndex(masses, (Double) p._1, tolPPM);
      if (index >= 0 && index < masses.length) {
        peaks.add(Optional.of(index));
        count++;
      } else {
        peaks.add(Optional.empty());
      }
    }
    this.matchingPeaks = peaks;
    this.matchingCount = count;
  }

}
