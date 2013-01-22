/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.lib.math.SumKeyVal;
import com.malhartech.lib.util.KeyValPair;
import com.malhartech.lib.util.MutableDouble;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class DailyVolume extends SumKeyVal<String, Long>
{
  private static final Logger logger = LoggerFactory.getLogger(DailyVolume.class);
  long windowId;
  protected transient HashMap<String, MutableDouble> csums = new HashMap<String, MutableDouble>();

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  @Override
  public void endWindow()
  {
    boolean dosum = sum.isConnected();
    boolean doaverage = average.isConnected();
    boolean docount = count.isConnected();

    if (dosum) {
      for (Map.Entry<String, MutableDouble> e: sums.entrySet()) {
        String key = e.getKey();
        MutableDouble val = csums.get(key);
        if (val == null) {
          val = new MutableDouble(e.getValue().value);
        }
        else {
          val.add(e.getValue().value);
        }
        sum.emit(new KeyValPair<String, Long>(cloneKey(key), getValue(val.value)));
        csums.put(cloneKey(key), val);

        if (docount) {
          count.emit(cloneCountTuple(key, new Integer(counts.get(e.getKey()).value)));
        }
        if (doaverage) {
          average.emit(cloneAverageTuple(key, getValue(e.getValue().value / counts.get(e.getKey()).value)));
        }
      }
    }
    else if (docount) { // sum is not connected, only counts is connected
      for (Map.Entry<String, MutableInteger> e: counts.entrySet()) {
        count.emit(cloneCountTuple(e.getKey(), new Integer(e.getValue().value)));
      }
    }

    sums.clear();
    counts.clear();

    //logger.debug(String.format("Window id: %d, dailyTotalVolume: %f", windowId, csums.get("YHOO").value));
  }
}
