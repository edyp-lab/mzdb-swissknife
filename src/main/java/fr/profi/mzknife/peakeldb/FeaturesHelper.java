package fr.profi.mzknife.peakeldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class FeaturesHelper {

  private final static Logger LOG = LoggerFactory.getLogger(FeaturesHelper.class);

  public static List<PutativeFeatureWrapper> groupByIonKeys(List<PutativeFeatureWrapper> features) {

    long start = System.currentTimeMillis();
    final Map<String, List<PutativeFeatureWrapper>> psmsByIonKey = features.stream().collect(Collectors.groupingBy(p -> p.getIonKey()));
    final List<PutativeFeatureWrapper> ions = new ArrayList<>(psmsByIonKey.size());

    for (Map.Entry<String, List<PutativeFeatureWrapper>> e : psmsByIonKey.entrySet()) {

      final List<PutativeFeatureWrapper> putativeFeatures = e.getValue();
      final List<PutativeFeatureWrapper> matchedPutativefeatures = putativeFeatures.stream().filter(PutativeFeatureWrapper::isMatched).collect(Collectors.toList());
      final Optional<PutativeFeatureWrapper> representativeFt = matchedPutativefeatures.stream().max(Comparator.comparing(ft -> ft.getRepresentativeExperimentalFeature().getBasePeakel().getApexIntensity()));

      if (representativeFt.isPresent()) {
        representativeFt.get().setGroupedFeatures(matchedPutativefeatures);
        ions.add(representativeFt.get());
      } else {
        LOG.info("No matched representative Feature found for this ion {}. Use the first non-matched as representative", e.getKey());
        ions.add(putativeFeatures.get(0));
      }
    }
    LOG.info("Grouping Duration : {} ms to generate {} ions", (System.currentTimeMillis() - start), ions.size());
    return ions;
  }

}
