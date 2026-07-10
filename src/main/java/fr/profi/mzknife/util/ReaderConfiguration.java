package fr.profi.mzknife.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class ReaderConfiguration {

  private final static Logger LOG = LoggerFactory.getLogger(ReaderConfiguration.class);
  private static final CharSequence DEFAULT_SEPARATOR = ";";

  public enum Column {ID, MOZ, CHARGE, RT, SEQ, PTMS, SCAN_NUMBER, TTOL, CV, RAWFILE, PROTEIN_GROUP, ABUNDANCE}

  public static class ColumnMapping {
    public final Column column;
    public int index;
    public List<Integer> indexes = null;
    public String columnName = null;
    public List<String> columnNames = null;

    public ColumnMapping(Column c) {
      column = c;
      index = c.ordinal();
    }

    public boolean isPresent() {
      return this.index >= 0;
    }
  }

  public final Map<Column, ColumnMapping> columns;
  public final CharSequence separator;

  protected ReaderConfiguration(Properties properties) {
    this.columns = initializeColumns(properties);
    this.separator = extractSeparator(properties);
  }

  public void updateNames(String[] header) {
    for (ColumnMapping mapping : columns.values()) {
      if (mapping.isPresent()) {
        if (mapping.index >= 0 && mapping.index < header.length) {
          mapping.columnName = header[mapping.index];
        }
        if (mapping.indexes != null) {
          mapping.columnNames = new ArrayList<>();
          for (Integer idx : mapping.indexes) {
            if (idx >= 0 && idx < header.length) {
              mapping.columnNames.add(header[idx]);
            } else {
              mapping.columnNames.add(null);
            }
          }
        }
      }
    }
  }

  /**
   * Factory method creating configuration from a path to the .columns file.
   */
  public static ReaderConfiguration fromFile(String configurationFilePath) {
    Properties properties = loadColumnProperties(configurationFilePath);
    return new ReaderConfiguration(properties);
  }

  private static CharSequence extractSeparator(Properties properties) {
    String value = properties.getProperty("SEPARATOR");
    if (value == null || value.isEmpty()) {
      return DEFAULT_SEPARATOR;
    }
    return value;
  }

  private static Properties loadColumnProperties(String configurationFilePath) {
    Properties properties = new Properties();

    if (configurationFilePath == null || configurationFilePath.isEmpty()) {
      return properties;
    }

    try (FileInputStream fis = new FileInputStream(configurationFilePath)) {
      properties.load(fis);
    } catch (IOException ioe) {
      LOG.error("Column properties cannot be read from {}", configurationFilePath, ioe);
    }

    return properties;
  }

  public static Map<Column, ColumnMapping> initializeColumns(Properties properties) {
    Map<Column, ColumnMapping> columns = new HashMap<>() {{
      for (Column c : Column.values()) {
        put(c, new ColumnMapping(c));
      }
    }};

    // initialize columns indexes by reading the configuration file (if supplied)
    for (Column c : columns.keySet()) {
      if (properties.containsKey(c.name())) {
        String value = properties.getProperty(c.name());
        if ((value != null) && !value.isEmpty()) {
          final String[] split = value.split("\\+");
          columns.get(c).index = Integer.parseInt(split[0].trim());
          if (split.length > 1) {
             columns.get(c).indexes = Stream.of(split).map(s -> Integer.valueOf(s.trim())).toList();
          }
        } else {
          columns.get(c).index = -1;
        }
      } else {
        columns.get(c).index = -1;
      }
    }
    return columns;
  }
}
