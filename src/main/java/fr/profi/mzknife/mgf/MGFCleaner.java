package fr.profi.mzknife.mgf;

import fr.profi.ms.model.TheoreticalIsotopePattern;
import fr.profi.mzdb.algo.DotProductPatternScorer;
import fr.profi.mzdb.model.SpectrumData;
import fr.profi.mzscope.InvalidMGFFormatException;
import fr.profi.mzscope.MSMSSpectrum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.util.*;

public class MGFCleaner extends MGFRewriter {

  private final static Logger LOG = LoggerFactory.getLogger(MGFCleaner.class);

  class Peak {

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
  
  
  public MGFCleaner(File srcFile, File m_dstFile) throws InvalidMGFFormatException {
    super(srcFile, m_dstFile);
  }

  protected MSMSSpectrum getSpectrum2Export(MSMSSpectrum inSpectrum){

    double tolPpm = 20.0;
    double[] masses = inSpectrum.getMassValues();
    double[] intensities = inSpectrum.getIntensityValues();
    float[] fIntensities = new float[intensities.length];

    List<Peak> peaks = new ArrayList<>(masses.length);
    List<Peak> result = new ArrayList<>(masses.length);
    Map<Integer, Peak> peaksByIndex = new HashMap<>();
    for (int k = 0; k < masses.length; k++) {
      Peak p = new Peak(masses[k], intensities[k], k);
      peaks.add(p);
      peaksByIndex.put(p.index, p);
      fIntensities[k] = (float)intensities[k];
    }

    SpectrumData spectrumData = new SpectrumData(masses, fIntensities);

    peaks.sort((o1, o2) -> Double.compare(o2.intensity, o1.intensity));

    for (int k = 0; k < peaks.size(); k++) {
      Peak p = peaks.get(k);
      if (!p.used) {
        Tuple2<Object, TheoreticalIsotopePattern> prediction = predictIsotopicPattern(spectrumData, p.mass, tolPpm);
        if ( (1e6*(prediction._2.monoMz() - p.mass)/p.mass) <= tolPpm ) {
          float intensity = 0;
          int charge = prediction._2.charge();
          for (Tuple2 t : prediction._2.mzAbundancePairs()) {
            Double mz = (Double) t._1;
            Float ab = (Float) t._2;
            int peakIdx = getPeakIndex(masses, mz, tolPpm);
            if ((peakIdx != -1) && (intensities[peakIdx] <= p.intensity)) {
              intensity+= intensities[peakIdx];
              peaksByIndex.get(peakIdx).used = true;
            } else {
              break;
            }
          }
          if ( (charge == 1) || (intensity == p.intensity) ) {
            Peak newPeak = new Peak(p.mass, intensity, p.index);
            result.add(newPeak);
          } else {
            Peak newPeak = new Peak(p.mass*charge - (charge-1)*1.00728, intensity, p.index);
//            LOG.info("Move peak ({},{}) to ({},{})", p.mass, p.intensity, newPeak.mass, newPeak.intensity);
            result.add(newPeak);
          }
        } else {
          p.used = true;
          result.add(new Peak(p));
        }
      }
    }

    result.sort(Comparator.comparingDouble(o -> o.mass));
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

  public static Tuple2<Object, TheoreticalIsotopePattern> predictIsotopicPattern(SpectrumData spectrum, double mz, double ppmTol) {
    Tuple2<Object, TheoreticalIsotopePattern>[] putativePatterns = DotProductPatternScorer.calcIsotopicPatternHypotheses(spectrum, mz, ppmTol);
    Tuple2<Object, TheoreticalIsotopePattern> bestPatternHypothese = DotProductPatternScorer.selectBestPatternHypothese(putativePatterns, 0.1);
    return bestPatternHypothese;
  }
}
