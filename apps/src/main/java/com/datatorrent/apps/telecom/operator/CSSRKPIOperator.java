package com.datatorrent.apps.telecom.operator;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.contrib.machinedata.data.AverageData;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TimeBucketKey;

public class CSSRKPIOperator implements Operator
{

  /**
   * This is the time interval for which the KPI is to be calculated. Currently it supports time intervals to be in
   * minutes. for e.g. 5m,10m,15m etc
   */
  @NotNull
  private int[] timeRange;

  /**
   * This is number of buckets to be stored for each time range.
   */
  @NotNull
  private int previousHistoryCount = 2;

  /**
   * This array stores the index from which the data needs to be emitted 
   */
  private int[] emitedTimeRangeIndex;
  private Map<Integer, List<KeyValPair<TimeBucketKey, AverageData>>> dataCache = new HashMap<Integer, List<KeyValPair<TimeBucketKey, AverageData>>>();

  public final transient DefaultOutputPort<KeyValPair<Integer, KeyValPair<TimeBucketKey, AverageData>>> outputPort = new DefaultOutputPort<KeyValPair<Integer, KeyValPair<TimeBucketKey, AverageData>>>();

  public final transient DefaultInputPort<KeyValPair<TimeBucketKey, AverageData>> inputPort = new DefaultInputPort<KeyValPair<TimeBucketKey, AverageData>>() {

    @Override
    public void process(KeyValPair<TimeBucketKey, AverageData> tuple)
    {
      int minute = tuple.getKey().getTime().get(Calendar.MINUTE);

      for (int i = 0; i < timeRange.length; i++) {
        int index = (minute / timeRange[i]) % previousHistoryCount;
        List<KeyValPair<TimeBucketKey, AverageData>> rangeBucket = dataCache.get(i);
        if (rangeBucket == null) {
          rangeBucket = new ArrayList<KeyValPair<TimeBucketKey, AverageData>>(previousHistoryCount);
          intializeArrayList(rangeBucket);
          rangeBucket.set(index, new KeyValPair<TimeBucketKey, AverageData>( tuple.getKey(), new AverageData(tuple.getValue().getSum(), tuple.getValue().getCount())));
          dataCache.put(i, rangeBucket);
        } else {
          if (rangeBucket.get(index) == null) { // the aggregations for new date is not present in the cache and
                                                // corresponding cell is empty
            rangeBucket.set(index, new KeyValPair<TimeBucketKey, AverageData>(tuple.getKey(), new AverageData(tuple.getValue().getSum(), tuple.getValue().getCount())));
          } else {
            KeyValPair<TimeBucketKey, AverageData> existingSum = rangeBucket.get(index);
            if (Math.abs(minute - existingSum.getKey().getTime().get(Calendar.MINUTE)) < timeRange[i]) {
              AverageData avData = existingSum.getValue();
              avData.setCount(avData.getCount() + tuple.getValue().getCount());
              avData.setSum(avData.getSum() + tuple.getValue().getSum());
            } else {
              outputPort.emit(new KeyValPair<Integer, KeyValPair<TimeBucketKey, AverageData>>(timeRange[i], existingSum));
              rangeBucket.set(index, new KeyValPair<TimeBucketKey, AverageData>(tuple.getKey(), new AverageData(tuple.getValue().getSum(), tuple.getValue().getCount())));

              if (emitedTimeRangeIndex[i] == index) {
                emitedTimeRangeIndex[i] = (index + 1) % previousHistoryCount;
              } else {
                int j = emitedTimeRangeIndex[i];
                while (j != index) {
                  outputPort.emit(new KeyValPair<Integer, KeyValPair<TimeBucketKey, AverageData>>(timeRange[i], rangeBucket.get(j)));
                  rangeBucket.set(j, null);
                  j = (j + 1) % previousHistoryCount;
                }
                emitedTimeRangeIndex[i] = j;
              }
            }
          }
        }
      }
    }

    private void intializeArrayList(List<KeyValPair<TimeBucketKey, AverageData>> rangeBucket)
    {
      for (int i = 0; i < previousHistoryCount; i++) {
        rangeBucket.add(null);
      }
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    emitedTimeRangeIndex = new int[timeRange.length];
    // Arrays.fill(emitedTimeRangeIndex, -1);
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {

  }

  public int[] getTimeRange()
  {
    return timeRange;
  }

  public void setTimeRange(int[] timeRange)
  {
    this.timeRange = timeRange;
  }

  public int getPreviousHistoryCount()
  {
    return previousHistoryCount;
  }

  public void setPreviousHistoryCount(int previousHistoryCount)
  {
    this.previousHistoryCount = previousHistoryCount;
  }

}
