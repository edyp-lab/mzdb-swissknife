package fr.profi.mzknife.util;

import org.junit.Test;
import java.util.Properties;
import static org.junit.Assert.*;

public class ReaderConfigurationTest {

    @Test
    public void testUpdateNames() {
        Properties props = new Properties();
        props.setProperty("PROTEIN_GROUP", "0");
        props.setProperty("ABUNDANCE", "1+2");
        
        ReaderConfiguration config = new ReaderConfigurationProxy(props);
        
        String[] header = {"Accession", "Sample1", "Sample2", "Other"};
        config.updateNames(header);
        
        ReaderConfiguration.ColumnMapping pgMapping = config.columns.get(ReaderConfiguration.Column.PROTEIN_GROUP);
        assertEquals("Accession", pgMapping.columnName);
        assertNull(pgMapping.columnNames);
        
        ReaderConfiguration.ColumnMapping abundanceMapping = config.columns.get(ReaderConfiguration.Column.ABUNDANCE);
        assertEquals("Sample1", abundanceMapping.columnName);
        assertNotNull(abundanceMapping.columnNames);
        assertEquals(2, abundanceMapping.columnNames.size());
        assertEquals("Sample1", abundanceMapping.columnNames.get(0));
        assertEquals("Sample2", abundanceMapping.columnNames.get(1));
    }

    @Test
    public void testUpdateNamesWithInvalidIndexes() {
        Properties props = new Properties();
        props.setProperty("PROTEIN_GROUP", "10"); // Out of bounds
        
        ReaderConfiguration config = new ReaderConfigurationProxy(props);
        
        String[] header = {"Accession"};
        config.updateNames(header);
        
        ReaderConfiguration.ColumnMapping pgMapping = config.columns.get(ReaderConfiguration.Column.PROTEIN_GROUP);
        assertNull(pgMapping.columnName);
    }

    private static class ReaderConfigurationProxy extends ReaderConfiguration {
        private ReaderConfigurationProxy(Properties properties) {
            super(properties);
        }
    }
}
