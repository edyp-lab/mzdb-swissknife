package fr.profi.mzknife;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import fr.profi.mzknife.util.AbstractProcessing;
import fr.profi.mzknife.util.ReaderConfiguration;
import fr.profi.mzknife.util.ReaderConfiguration.Column;
import fr.profi.mzknife.util.ReaderConfiguration.ColumnMapping;
import fr.profi.util.math.RatioFitting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class RollupProcessing extends AbstractProcessing {

   private final static Logger LOG = LoggerFactory.getLogger(RollupProcessing.class);

   public static void main(String[] args) throws Exception {
      CommandArguments.ProteinRollupCommand proteinRollupCommand = new CommandArguments.ProteinRollupCommand();
      addCommand(proteinRollupCommand);

      try {
         String parsedCommand = parseCommand(args);
         if (parsedCommand.equals(CommandArguments.PROTEIN_ROLLUP_COMMAND_NAME)) {
            if (proteinRollupCommand.help) usage();
            rollup(proteinRollupCommand);
         } else {
            LOG.warn("Invalid command specified ");
            usage();
         }
      } catch (FileNotFoundException fnfe) {
         LOG.error("File not found", fnfe);
      } catch (Exception e) {
         LOG.error("ERROR ", e);
      }
   }

   public static void rollup(CommandArguments.ProteinRollupCommand command) throws IOException, CsvValidationException {
      ReaderConfiguration configuration = ReaderConfiguration.fromFile(command.columnsConfig);

      ColumnMapping proteinGroupMapping = configuration.columns.get(Column.PROTEIN_GROUP);
      ColumnMapping abundanceMapping = configuration.columns.get(Column.ABUNDANCE);
      ColumnMapping idMapping = configuration.columns.get(Column.ID);
      ColumnMapping sequenceMapping = configuration.columns.get(Column.SEQ);
      ColumnMapping chargeMapping = configuration.columns.get(Column.CHARGE);

      if (!proteinGroupMapping.isPresent()) {
         LOG.error("PROTEIN_GROUP column must be provided in the configuration file");
         return;
      }
      if (!abundanceMapping.isPresent()) {
         LOG.error("ABUNDANCE column must be provided in the configuration file");
         return;
      }

      List<Integer> abundanceIndexes = abundanceMapping.indexes;
      if (abundanceIndexes == null) {
         abundanceIndexes = Collections.singletonList(abundanceMapping.index);
      }

      File inputFile = new File(command.inputFile);
      File outputFile = new File(command.outputFile);

      Map<String, List<Ion>> ionsByProteinGroup = new HashMap<>();
      List<Float>[] abundancesBySamples = new List[abundanceIndexes.size()];
      for(int i = 0; i < abundancesBySamples.length; i++) {
         abundancesBySamples[i] = new ArrayList<>();
      }

      char separator = configuration.separator.charAt(0);
      final CSVParser parser = new CSVParserBuilder().withSeparator(separator).build();
      try (CSVReader reader = new CSVReaderBuilder(new FileReader(inputFile)).withCSVParser(parser).build()) {
         String[] header = reader.readNext();
         if (header == null) {
            LOG.error("Input file is empty");
            return;
         }
         configuration.updateNames(header);

         String[] line;
         int lineCount = 1;
         while ((line = reader.readNext()) != null) {
            lineCount++;
            try {
               String proteinGroup = line[proteinGroupMapping.index];

               String id = idMapping.isPresent() ? line[idMapping.index] : "";
               String sequence = sequenceMapping.isPresent() ? line[sequenceMapping.index] : "";
               int charge = chargeMapping.isPresent() ? Integer.parseInt(line[chargeMapping.index]) : 0;

               Ion ion = new Ion(id, sequence, charge, abundanceIndexes.size());
               for (int i = 0; i < abundanceIndexes.size(); i++) {

                  int colIndex = abundanceIndexes.get(i);
                  if (colIndex < line.length) {
                     String abundanceStr = line[colIndex];
                     if (abundanceStr != null && !abundanceStr.isEmpty()) {
                        ion.abundances[i] = Float.parseFloat(abundanceStr);
                        if (ion.abundances[i] > 0.0f) {
                           abundancesBySamples[i].add(ion.abundances[i]);
                        } else {
                           ion.abundances[i] = Float.NaN;
                        }
                     } else {
                        ion.abundances[i] = Float.NaN;
                     }
                  }
               }

               ionsByProteinGroup.computeIfAbsent(proteinGroup, k -> new ArrayList<>()).add(ion);

            } catch (NumberFormatException nfe) {
               LOG.warn("Line {}: Cannot parse some numeric value", lineCount);
            } catch (ArrayIndexOutOfBoundsException e) {
               LOG.error("Line {}: Column index out of bounds", lineCount);
            }
         }
      }


      Map<String, ProteinGroup> aggregatedProteinGroups = new TreeMap<>();
      for (Map.Entry<String, List<Ion>> entry : ionsByProteinGroup.entrySet()) {
         String proteinGroupAccession = entry.getKey();
         List<Ion> ions = entry.getValue();
         ProteinGroup proteinGroup = new ProteinGroup(proteinGroupAccession, ions.size(), abundanceIndexes.size());

         float[][] pgIonsAbundances = new float[ions.size()][];

         for (int i = 0; i < abundanceIndexes.size(); i++) {
            for (int j = 0; j < ions.size(); j++) {
               pgIonsAbundances[j] = ions.get(j).abundances;
            }
         }
         proteinGroup.abundances = aggregate(pgIonsAbundances, command.aggregationMethod);
         aggregatedProteinGroups.put(proteinGroupAccession, proteinGroup);
      }

      try (ICSVWriter writer = new CSVWriterBuilder(new FileWriter(outputFile))
              .withSeparator(separator)
              .build()) {

         List<String> outputHeader = new ArrayList<>();
         outputHeader.add("Protein Group");
         outputHeader.add("Nb Ions");
         if (abundanceMapping.indexes != null && abundanceMapping.indexes.size() > 1) {
            for (int i = 0; i < abundanceIndexes.size(); i++) {
               if (abundanceMapping.columnNames != null && i < abundanceMapping.columnNames.size() && abundanceMapping.columnNames.get(i) != null) {
                  outputHeader.add(abundanceMapping.columnNames.get(i));
               } else {
                  outputHeader.add("Abundance_" + (i + 1));
               }
            }
         } else {
            if (abundanceMapping.columnName != null) {
               outputHeader.add(abundanceMapping.columnName);
            } else {
               outputHeader.add("Abundance");
            }
         }

         writer.writeNext(outputHeader.toArray(new String[0]));
         for (Map.Entry<String, ProteinGroup> entry : aggregatedProteinGroups.entrySet()) {
            List<String> outputLine = new ArrayList<>();
            ProteinGroup pg = entry.getValue();
            outputLine.add(pg.accession);
            outputLine.add(String.valueOf(pg.nbIons));
            for (float val : pg.abundances) {
               outputLine.add(String.valueOf(val));
            }
            writer.writeNext(outputLine.toArray(new String[0]));
         }
      }

      LOG.info("Protein rollup completed. Results written to {}", outputFile.getAbsolutePath());
   }

   private static float[] aggregate(float[][] values, String aggregationMethod) {

      if (values == null || values.length == 0) return null;

      if (aggregationMethod.equalsIgnoreCase("MRF")) {
       return RatioFitting.fit(values);
      }


      int N = values[0].length;
      float[] result = new float[N];
      for (int i = 0; i < N; i++) {

         List<Float> filteredValues = new ArrayList<>();
         for (float[] v : values) {
            if (!Float.isNaN(v[i]) && v[i] > 0) {
               filteredValues.add(v[i]);
            }
         }

         if (filteredValues.isEmpty()) result[i] = 0.0f;

         switch (aggregationMethod.toLowerCase()) {
            case "sum":
               float sum = 0;
               for (float v : filteredValues) sum += v;
               result[i] = sum;
               break;
            case "mean":
               float total = 0;
               for (float v : filteredValues) total += v;
               result[i] = total / filteredValues.size();
               break;
            case "median":
               Collections.sort(filteredValues);
               int size = filteredValues.size();
               if (size % 2 == 0) {
                  result[i] = (filteredValues.get(size / 2 - 1) + filteredValues.get(size / 2)) / 2.0f;
               } else {
                  result[i] = filteredValues.get(size / 2);
               }
               break;
            case "max":
               float max = -Float.MAX_VALUE;
               for (float v : filteredValues) if (v > max) max = v;
               result[i] = max;
               break;
            default:
               LOG.warn("Unknown aggregation method '{}', defaulting to sum", aggregationMethod);
               float defaultSum = 0;
               for (float v : filteredValues) defaultSum += v;
               result[i] = defaultSum;
         }
      }
      return result;
   }

   public static class Ion {
      public String id;
      public String sequence;
      public int charge;
      public float[] abundances;

      public Ion(String id, String sequence, int charge, int nbAbundances) {
         this.id = id;
         this.sequence = sequence;
         this.charge = charge;
         this.abundances = new float[nbAbundances];
      }
   }

   public static class ProteinGroup {
      public int nbIons;
      public String accession;
      public float[] abundances;

      public ProteinGroup(String accession, int nbIons, int nbAbundances) {
         this.accession = accession;
         this.nbIons = nbIons;
         this.abundances = new float[nbAbundances];
      }
   }
}
