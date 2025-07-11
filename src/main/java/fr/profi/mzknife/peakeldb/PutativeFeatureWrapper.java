package fr.profi.mzknife.peakeldb;

import fr.profi.mzdb.model.Feature;
import fr.profi.mzdb.model.PutativeFeature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class PutativeFeatureWrapper extends PutativeFeature {

  public enum Type { PROVIDED, CROSS_ASSIGNED}
  private Type type = Type.PROVIDED;
  private String ionKey;
  private String sequence;
  private String modification;
  private String rawSourceFile;

  private Feature representativeExperimentalFt;
  private List<Feature> experimentalFeatures = new ArrayList<>(5);
  private List<PutativeFeatureWrapper> groupedFeatures = null;
  private Optional<Boolean> isReliable = Optional.empty();

  private String peptideKey;
  private String cvValue = null;

  public PutativeFeatureWrapper(int id, double mz, int charge) {
    super(id, mz, charge);
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public String getSequence() {
    return sequence;
  }

  public String getModification() {
    return modification;
  }

  public String getIonKey() {
    return ionKey;
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

  public void setGroupedFeatures(List<PutativeFeatureWrapper> groupedFeatures) {
    this.groupedFeatures = groupedFeatures;
  }

  public List<PutativeFeatureWrapper> getGroupedFeatures() {
    return groupedFeatures;
  }

  public List<Integer> getGroupedPeakelIds() {
    if (groupedFeatures == null)
      return null;
    return groupedFeatures.stream().flatMap(pf -> pf.getExperimentalFeatures().stream().flatMap(f -> Arrays.stream(f.getPeakels()).map(peakel -> peakel.getId()))).distinct().toList();
  }

  public Feature getRepresentativeExperimentalFeature() {
    return representativeExperimentalFt;
  }

  public void setSequenceModifications(String sequence, String modification) {
    this.sequence = sequence;
    this.modification = modification;
    updateKeys(false);
  }

  public void updateKeys(boolean useCvValue) {
    StringBuilder stb = new StringBuilder(this.sequence);
    if (this.modification != null) {
      stb.append('.').append(this.modification);
    }
    this.peptideKey = stb.toString();
    stb.append('.').append(charge());
    if (useCvValue && this.cvValue != null) {
      stb.append('.').append(this.cvValue);
    }
    this.ionKey = stb.toString();
  }

  public Optional<Boolean> isReliable() {
    return isReliable;
  }

  public void setIsReliable(Boolean isReliable) {
    this.isReliable = Optional.of(isReliable);
  }

  public void setCvValue(String cvValue) {
    this.cvValue = cvValue;
  }

  public String getCvValue() {
    return cvValue;
  }

  public String getRawSourceFile() {
    return rawSourceFile;
  }

  public void setRawSourceFile(String rawSourceFile) {
    this.rawSourceFile = rawSourceFile;
  }
}
