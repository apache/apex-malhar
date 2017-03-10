package org.apache.apex.malhar.lib.fs;

import org.joda.time.Duration;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.SumAccumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class WindowedWordCount implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    LineByLineFileInputOperator input = dag.addOperator("input", new LineByLineFileInputOperator());
    input.setDirectory("/tmp/input");
    input.setEmitBatchSize(1);
    StringToKeyValPair convert1 = dag.addOperator("convert1", new StringToKeyValPair());
    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> windowedOperator =
      dag.addOperator("count", new KeyedWindowedOperatorImpl());
    Accumulation<Long, MutableLong, Long> sum = new SumAccumulation();

    windowedOperator.setAccumulation(sum);
    windowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<String, MutableLong>());
    windowedOperator.setRetractionStorage(new InMemoryWindowedKeyedStorage<String, Long>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.millis(1)));
    windowedOperator.setTriggerOption(TriggerOption.AtWatermark());

    KeyValPairToString convert2 = dag.addOperator("convert2", new KeyValPairToString());

    FileOutputOperator output = dag.addOperator("output", new FileOutputOperator());
    output.setFilePath("/tmp/output");

    dag.addStream("inputToConvert1", input.output, convert1.input);
    dag.addStream("convert1ToCount", convert1.output, windowedOperator.input);
    dag.addStream("countToConvert2", windowedOperator.output, convert2.input);
    dag.addStream("convert2ToOutput", convert2.output, output.input);
  }

  public static class FileOutputOperator extends AbstractFileOutputOperator<String>
  {

    @Override
    protected String getFileName(String tuple)
    {
      return currentFileName;
    }

    @Override
    protected byte[] getBytesForTuple(String tuple)
    {
      return tuple.getBytes();
    }
  }

  @Test
  public void testWordCount()
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    try {
      lma.prepareDAG(new WindowedWordCount(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(true);
      lc.run(1000*1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
