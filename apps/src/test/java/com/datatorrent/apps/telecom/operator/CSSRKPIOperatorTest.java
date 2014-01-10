package com.datatorrent.apps.telecom.operator;

import java.util.Calendar;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.machinedata.data.AverageData;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TimeBucketKey;

public class CSSRKPIOperatorTest
{

  @Test
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator()
  {
    CSSRKPIOperator oper = new CSSRKPIOperator();
    int[] timeRange = new int[] { 1,2 };
    oper.setTimeRange(timeRange);
    oper.setPreviousHistoryCount(2);

    CollectorTestSink sortSink = new CollectorTestSink();
    oper.outputPort.setSink(sortSink);

    oper.setup(null);
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 0);
    oper.beginWindow(0);
    for (int i = 0; i < 2; i++) {
      oper.inputPort.process(new KeyValPair<TimeBucketKey, AverageData>(new TimeBucketKey(cal, TimeBucketKey.TIMESPEC_MINUTE_SPEC), new AverageData(i, i)));
    }
    oper.endWindow();
    cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 1);
    oper.beginWindow(1);
    for (int i = 0; i < 2; i++) {
      oper.inputPort.process(new KeyValPair<TimeBucketKey, AverageData>(new TimeBucketKey(cal, TimeBucketKey.TIMESPEC_MINUTE_SPEC), new AverageData(i, i)));
    }
    oper.endWindow();
    /*
    cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 2);
    oper.beginWindow(2);
    for (int i = 0; i < 2; i++) {
      oper.inputPort.process(new KeyValPair<TimeBucketKey, AverageData>(new TimeBucketKey(cal, TimeBucketKey.TIMESPEC_MINUTE_SPEC), new AverageData(i, i)));
    }
    oper.endWindow();
    */
    
    cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 3);
    oper.beginWindow(3);
    for (int i = 0; i < 2; i++) {
      oper.inputPort.process(new KeyValPair<TimeBucketKey, AverageData>(new TimeBucketKey(cal, TimeBucketKey.TIMESPEC_MINUTE_SPEC), new AverageData(i, i)));
    }
    oper.endWindow();
    cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 4);
    oper.beginWindow(4);
    for (int i = 0; i < 2; i++) {
      oper.inputPort.process(new KeyValPair<TimeBucketKey, AverageData>(new TimeBucketKey(cal, TimeBucketKey.TIMESPEC_MINUTE_SPEC), new AverageData(i, i)));
    }
    oper.endWindow();
    cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 5);
    oper.beginWindow(5);
    for (int i = 0; i < 2; i++) {
      oper.inputPort.process(new KeyValPair<TimeBucketKey, AverageData>(new TimeBucketKey(cal, TimeBucketKey.TIMESPEC_MINUTE_SPEC), new AverageData(i, i)));
    }
    oper.endWindow();
    
    cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 6);
    oper.beginWindow(6);
    for (int i = 0; i < 2; i++) {
      oper.inputPort.process(new KeyValPair<TimeBucketKey, AverageData>(new TimeBucketKey(cal, TimeBucketKey.TIMESPEC_MINUTE_SPEC), new AverageData(i, i)));
    }
    oper.endWindow();
    
    for (Object o : sortSink.collectedTuples) {
      System.out.println(o.toString());
      logger.info(o.toString());
    }
  }

  private static Logger logger = LoggerFactory.getLogger(CSSRKPIOperatorTest.class);
}
