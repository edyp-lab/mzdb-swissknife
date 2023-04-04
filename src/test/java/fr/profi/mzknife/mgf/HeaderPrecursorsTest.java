package fr.profi.mzknife.mgf;

import com.almworks.sqlite4java.SQLiteException;
import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.IsolationWindowParamTree;
import fr.profi.mzdb.db.model.params.param.CVEntry;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.model.params.param.UserParam;
import fr.profi.mzdb.model.SpectrumHeader;
import fr.profi.util.metrics.Metric;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;

@Ignore
public  class HeaderPrecursorsTest {

  final static Logger logger = LoggerFactory.getLogger(HeaderPrecursorsTest.class);
  private Metric metric = new Metric("HeaderPrecursorsTest");


  @Test
  public void testHeaders()  {
    long start = System.currentTimeMillis();

    String mzdbFilePath = "C:/Local/bruley/Data/Proline/Data/mzdb/Exploris/Xpl1_002790.mzDB";

    try {

      MzDbReader mzDbReader = new MzDbReader(mzdbFilePath, true);
      mzDbReader.enablePrecursorListLoading();
      mzDbReader.enableScanListLoading();


      for (SpectrumHeader sh : mzDbReader.getMs2SpectrumHeaders()) {

        if (sh.getScanList() == null) sh.loadScanList(mzDbReader.getConnection());

        metric.incr("ms2_scan");

        double headerMz = sh.getPrecursorMz();
        double precursorMz = sh.getPrecursorMz();

        IsolationWindowParamTree iw = sh.getPrecursor().getIsolationWindow();

        CVEntry[] cvEntries = {CVEntry.ISOLATION_WINDOW_LOWER_OFFSET, CVEntry.ISOLATION_WINDOW_TARGET_MZ, CVEntry.ISOLATION_WINDOW_UPPER_OFFSET};
        CVParam[] cvParams = iw.getCVParams(cvEntries);

        float lowerMzOffset = Float.parseFloat(cvParams[0].getValue());
        float swCenterMz = Float.parseFloat(cvParams[1].getValue());
        float upperMzOffset = Float.parseFloat(cvParams[2].getValue());


        UserParam thermoPrecMzParam = sh.getScanList().getScans().get(0).getUserParam("[Thermo Trailer Extra]Monoisotopic M/Z:");
        double thermoRefinedPrecMz = Double.parseDouble(thermoPrecMzParam.getValue());
        if (thermoRefinedPrecMz <= 0.0) {
          metric.incr("no_thermo_mz_value");
        }


        metric.addValue("header_prec", Math.abs(1e6 * (headerMz - precursorMz) / headerMz));

        if (Math.abs(1e6 * (headerMz - swCenterMz) / headerMz) > 10.0) {
          metric.addValue("swTarget_prec", Math.abs(1e6 * (headerMz - swCenterMz) / headerMz));
        }

        logger.info("scan={}; header.mz={}; precursor.mz={}; sw_center.mz={}; thermo_refined.mz={}; z={}+", sh.getId(), headerMz, precursorMz, swCenterMz, thermoRefinedPrecMz, sh.getPrecursorCharge());

        if ((thermoRefinedPrecMz > 0.0)) {
          metric.addValue("thermo_prec", Math.abs(1e6 * (headerMz - thermoRefinedPrecMz) / headerMz));
        }

      }

      mzDbReader.close();

      float took = (System.currentTimeMillis() - start) / 1000f;
      logger.info("extraction took: " + took);
      logger.info(metric.toString());

    } catch (SQLiteException e) {
      throw new RuntimeException(e);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }


}
