package fr.profi.mzknife.mgf;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {
  public static void dumpStats(Map<String, Object> map, BufferedWriter fw, String[] annotations) throws IOException {
    fw.write(Arrays.stream(annotations).map(k -> map.getOrDefault(k, "").toString()).collect(Collectors.joining("\t")));
    fw.newLine();
  }
}
