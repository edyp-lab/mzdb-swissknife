package fr.profi.mzknife.peakeldb;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsensusIon {

  Map<String, List<PutativeFeatureWrapper>> putativesFeaturesByRun;
  Map<String, List<PutativeFeatureWrapper>> missingFeaturesByRun;

  PutativeFeatureWrapper representativeFeature;

  public ConsensusIon(List<PutativeFeatureWrapper> putativesFeatures) {
    this.putativesFeaturesByRun = putativesFeatures.stream().collect(Collectors.groupingBy(PutativeFeatureWrapper::getRawSourceFile));
    this.missingFeaturesByRun = new HashMap<>();

    // select representative Feature
    final List<PutativeFeatureWrapper> matchedFeatures = getMatchedFeatures();
    if (!matchedFeatures.isEmpty()) {
      representativeFeature = matchedFeatures.get(0);
    } else {
      representativeFeature = putativesFeatures.get(0);
    }
  }

  public List<String> getMatchedRuns() {
    return putativesFeaturesByRun.keySet().stream().distinct().sorted().toList();
  }

  public List<PutativeFeatureWrapper> getMatchedFeatures() {
    return putativesFeaturesByRun.values().stream().flatMap(Collection::stream).filter(PutativeFeatureWrapper::isMatched).collect(Collectors.toList());
  }

  public PutativeFeatureWrapper getRepresentativeFeature() {
    return representativeFeature;
  }

  public void addMissingFeature(String run, PutativeFeatureWrapper missingFeature) {
    missingFeaturesByRun.computeIfAbsent(run, x -> new ArrayList<>()).add(missingFeature);
  }

  public List<PutativeFeatureWrapper> getPutativeFeatures(String run) {
    return putativesFeaturesByRun.containsKey(run) ? putativesFeaturesByRun.get(run) : missingFeaturesByRun.getOrDefault(run, null);
  }

  public List<PutativeFeatureWrapper> getAllPutativeFeatures() {
    return Stream.concat( putativesFeaturesByRun.values().stream().flatMap(Collection::stream),
                          missingFeaturesByRun.values().stream().flatMap(Collection::stream)).toList();
  }


}
