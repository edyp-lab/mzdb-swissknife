package fr.profi.mzknife.peakeldb;

import fr.profi.mzdb.model.Feature;
import fr.profi.mzdb.model.PutativeFeature;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PutativeFeatureWrapper extends PutativeFeature {

  private String key;
  private String sequence;
  private String modification;
  private Feature representativeExperimentalFt;
  private List<Feature> experimentalFeatures;
  private Optional<Boolean> isReliable = Optional.empty();

  private String peptideKey;

  public PutativeFeatureWrapper(int id, double mz, int charge) {
    super(id, mz, charge);
    experimentalFeatures = new ArrayList<>(5);
  }

  public String getSequence() {
    return sequence;
  }

  public String getModification() {
    return modification;
  }

  public String getKey() {
    return key;
  }

  public String getPeptideKey() {
    return peptideKey;
  }

  public List<Feature> getExperimentalFeatures() {
    return experimentalFeatures;
  }

  public boolean addExperimentalFeature(Feature feature) {
     boolean added = experimentalFeatures.add(feature);
     if (added) {
       if ((representativeExperimentalFt == null) || (feature.getBasePeakel().getApexIntensity() > representativeExperimentalFt.getBasePeakel().getApexIntensity())) {
         representativeExperimentalFt = feature;
       }
     }
     return added;
  }

  public Feature getRepresentativeExperimentalFeature() {
    return representativeExperimentalFt;
  }

  public void setSequenceModifications(String sequence, String modification) {
    this.sequence = sequence;
    this.modification = modification;
    StringBuilder stb = new StringBuilder(this.sequence);
    if (modification != null) {
      stb.append('.').append(this.modification);
    }
    this.peptideKey = stb.toString();
    stb.append('.').append(charge());
    this.key = stb.toString();
  }

  public Optional<Boolean> isReliable() {
    return isReliable;
  }

  public void setIsReliable(Boolean isReliable) {
    this.isReliable = Optional.of(isReliable);
  }


}
