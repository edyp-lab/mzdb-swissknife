package fr.profi.mzknife.peakeldb;

import fr.profi.mzdb.model.Feature;
import fr.profi.mzdb.model.Peakel;
import scala.Tuple2;

public class FeatureAdapter extends Feature {

  private Peakel basePeakel;
  private int basePeakelIndex = 0;

  public FeatureAdapter(int id, double mz, int charge, Tuple2<Peakel, Object>[] indexedPeakels, boolean isPredicted, long[] ms2SpectrumIds) {
    super(id, mz, charge, indexedPeakels, isPredicted, ms2SpectrumIds);

    double approxMass = mz * charge;
    int theoBasePeakelIndex = (approxMass < 2000) ?  0 :  (approxMass < 3500) ? 1 : 2;

    for (Tuple2<Peakel, Object> t :  indexedPeakels ) {
      Integer isotopeIdx = (Integer)t._2;
      if (isotopeIdx <= theoBasePeakelIndex) {
        basePeakelIndex = isotopeIdx;
        basePeakel = t._1;
      }
    }
  }

  @Override
  public Peakel getBasePeakel() {
    return basePeakel;
  }

  @Override
  public int getBasePeakelIndex() {
    return basePeakelIndex;
  }

  @Override
  public float getElutionTime() {
    return super.getBasePeakel().getElutionTime();
  }
}
