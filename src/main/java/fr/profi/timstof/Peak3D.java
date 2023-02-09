package fr.profi.timstof;

import java.util.Comparator;

public class Peak3D implements Comparable<Peak3D> {

  static final Comparator<Peak3D> massComparator = Comparator.comparingDouble((Peak3D peak) -> peak.mass);

  public double mass;
  public float intensity;
  public short mobilityIndex;

  public Peak3D(double mass, float intensity, short mobilityIndex) {
    this.mass = mass;
    this.intensity = intensity;
    this.mobilityIndex = mobilityIndex;
  }

  @Override
  public int compareTo(Peak3D p) {
    return massComparator.compare(this, p);
  }
}
