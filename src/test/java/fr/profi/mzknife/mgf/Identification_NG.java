package fr.profi.mzknife.mgf;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Identification_NG {
  Integer scan;
  Long bestQuery;
  Double bestScore;
  Double bestMoz;
  Integer bestCharge;
  Integer bestDecoyCount = 0;
  Integer bestTargetCount = 0;
  Integer bestTDCount = 0;
  Long worstQuery;
  Double worstScore;
  Double worstMoz;
  Integer worstCharge;
  Integer worstDecoyCount = 0;
  Integer worstTargetCount = 0;
  Integer worstTDCount = 0;

  public Identification_NG(Integer scan, Long bestQuery, Double bestScore, Double bestMoz, Integer bestCharge, Integer bestDecoyCount, Integer bestTargetCount, Integer bestTDCount, Long worstQuery, Double worstScore, Double worstMoz, Integer worstCharge, Integer worstDecoyCount, Integer worstTargetCount, Integer worstTDCount) {
    this.scan = scan;
    this.bestQuery = bestQuery;
    this.bestScore = bestScore;
    this.bestMoz = bestMoz;
    this.bestCharge = bestCharge;
    this.bestDecoyCount = bestDecoyCount;
    this.bestTargetCount = bestTargetCount;
    this.bestTDCount = bestTDCount;
    this.worstQuery = worstQuery;
    this.worstScore = worstScore;
    this.worstMoz = worstMoz;
    this.worstCharge = worstCharge;
    this.worstDecoyCount = worstDecoyCount;
    this.worstTargetCount = worstTargetCount;
    this.worstTDCount = worstTDCount;
  }

  public boolean isTarget()  {
    return (bestTargetCount > 0) && (bestDecoyCount == 0) && (bestTDCount == 0);
  }

  public boolean isDubious() {
    return (bestTDCount > 0) || ((bestTargetCount > 0) && (bestDecoyCount > 0));
  }

  public boolean isDecoy() {
    return (bestDecoyCount > 0) && (bestTargetCount == 0) && (bestTDCount == 0);
  }

  public boolean isWorstTarget() {
    return (worstTargetCount > 0) && (worstDecoyCount <= 0)  && (worstTDCount <= 0);
  }

  public boolean isWorstDubious() {
    return (worstTDCount > 0) || ((worstTargetCount > 0) && (worstDecoyCount > 0));
  }

  public boolean isWorstDecoy() {
    return (worstDecoyCount > 0) && (worstTargetCount <= 0) && (worstTDCount <= 0);
  }

  public String status() {
    if (isTarget()) {
      return "TARGET";
    } else if (isDubious()) {
      return "DUBIOUS";
    } else {
      return "DECOY";
    }
  }

  public String duplicatedStatus() {
    if (isWorstTarget()) {
      return "TARGET";
    } else if (isWorstDubious()) {
      return "DUBIOUS";
    } else if (isWorstDecoy()) {
      return "DECOY";
    } else {
      return "";
    }
  }

  public static List<Identification_NG> fromFile(String filename) throws IOException {

    List<String> lines = Files.readAllLines(new File(filename).toPath(), Charset.defaultCharset());

    List<Identification_NG> idents = lines.stream().skip(1).map(line -> {
              List<String> values = Arrays.stream(line.split(";")).map(l -> l.trim()).collect(Collectors.toList());
              return new Identification_NG(
                      Converter.toInt(values.get(0)).get(),
                      Converter.toLong(values.get(1)).get(),
                      Converter.toDouble(values.get(2)).get(),
                      Converter.toDouble(values.get(3)).get(),
                      Converter.toInt(values.get(4)).get(),
                      Converter.toInt(values.get(5)).get(),
                      Converter.toInt(values.get(6)).get(),
                      Converter.toInt(values.get(7)).get(),
                      Converter.toLong(values.get(10)).get(),
                      Converter.toDouble(values.get(11)).get(),
                      Converter.toDouble(values.get(8)).get(),
                      Converter.toInt(values.get(9)).get(),
                      Converter.toInt(values.get(12)).get(),
                      Converter.toInt(values.get(13)).get(),
                      Converter.toInt(values.get(14)).get());
            }
    ).collect(Collectors.toList());

    return idents;
  }

  public static class Converter {
    public static Optional<Integer> toInt(String s) {
      try {
        return Optional.of(Integer.parseInt(s));
      } catch (NumberFormatException e) {
        return Optional.empty();
      }
    }

    public static Optional<Long> toLong(String s) {
      try {
        return Optional.of(Long.parseLong(s));
      } catch (NumberFormatException e) {
        return Optional.empty();
      }
    }

    public static Optional<Double> toDouble(String s)  {
      try {
        return Optional.of(Double.parseDouble(s));
      } catch (NumberFormatException e) {
        return Optional.empty();
      }
    }

  }


}
