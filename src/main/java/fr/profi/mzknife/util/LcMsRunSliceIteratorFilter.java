package fr.profi.mzknife.util;

import fr.profi.mzdb.db.model.params.param.CVEntry;
import fr.profi.mzdb.model.RunSlice;
import fr.profi.mzdb.model.RunSliceData;
import fr.profi.mzdb.model.SpectrumSlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;

public class LcMsRunSliceIteratorFilter implements Iterator<RunSlice> {

  final static Logger logger = LoggerFactory.getLogger(LcMsRunSliceIteratorFilter.class);

  final Iterator<RunSlice> internalIterator;
  String cvValue;

  public LcMsRunSliceIteratorFilter(Iterator<RunSlice> iterator, String value) {
    internalIterator = iterator;
    cvValue = value.trim();
  }

  @Override
  public boolean hasNext() {
    return internalIterator.hasNext();
  }

  public RunSlice next() {
    RunSlice rs = internalIterator.next();

    final SpectrumSlice[] slices = rs.getData().getSpectrumSliceList();
    final SpectrumSlice[] filteredSlices = Arrays.stream(slices).filter(sl -> sl.getHeader().getCVParam(CVEntry.FAIMS_COMPENSATION_VOLTAGE).getValue().equals(cvValue)).toArray(SpectrumSlice[]::new);
    RunSliceData rsd = new RunSliceData(rs.getData().getId(), filteredSlices);

    return new RunSlice(rs.getHeader(), rsd);
  }

  @Override
  public void remove() {
    internalIterator.remove();
  }

  @Override
  public void forEachRemaining(Consumer<? super RunSlice> action) {
    internalIterator.forEachRemaining(action);
  }


}

