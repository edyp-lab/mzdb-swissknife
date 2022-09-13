package fr.profi.mzknife.mgf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class Identification {
  Integer scan;
  Double moz;
  Integer charge;
  Double initMmoz;
  Integer initCharge;
  String status;
  Double scoreMax;
  String scoreFrom;

  public Identification(Integer scan, Double moz, Integer charge, Double initMmoz, Integer initCharge, String status, Double scoreMax, String scoreFrom) {
    this.scan = scan;
    this.moz = moz;
    this.charge = charge;
    this.initMmoz = initMmoz;
    this.initCharge = initCharge;
    this.status = status;
    this.scoreMax = scoreMax;
    this.scoreFrom = scoreFrom;
  }


  public static List<Identification> fromFile(String filename) throws IOException {

    List<String> lines = Files.readAllLines(new File(filename).toPath(), Charset.defaultCharset());

    // yield Identification(values(0).split("_")(1).toInt, values(1).toDouble, values(2).toInt, values(3).toDouble, values(4).toInt, values(5), values(6).toDouble, values(7))

    List<Identification> idents = lines.stream().skip(1).map(line -> {
              List<String> values = Arrays.stream(line.split(";")).map(l -> l.trim()).collect(Collectors.toList());
              return new Identification(
                      Converter.toInt(values.get(0).split("_")[1]).get(),
                      Converter.toDouble(values.get(1)).get(),
                      Converter.toInt(values.get(2)).get(),
                      Converter.toDouble(values.get(3)).get(),
                      Converter.toInt(values.get(4)).get(),
                      values.get(5),
                      Converter.toDouble(values.get(6)).get(),
                      values.get(7));
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
