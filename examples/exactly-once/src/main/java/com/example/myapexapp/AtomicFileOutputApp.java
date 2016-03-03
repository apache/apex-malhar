/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.example.myapexapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.util.KeyValPair;

@ApplicationAnnotation(name = "AtomicFileOutput")
public class AtomicFileOutputApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput",
        new KafkaSinglePortStringInputOperator());
    kafkaInput.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());

    Application.UniqueCounterFlat count = dag.addOperator("count", new Application.UniqueCounterFlat());

    FileWriter fileWriter = dag.addOperator("fileWriter", new FileWriter());

    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("words", kafkaInput.outputPort, count.data);
    dag.addStream("counts", count.counts, fileWriter.input, cons.input);
  }

  /**
   * This implementation of {@link AbstractFileOutputOperator} writes to a single file. However when it doesn't
   * receive any tuples in an application window then it finalizes the file, i.e., the file is completed and will not
   * be opened again.
   * <p/>
   * If more tuples are received after a hiatus then they will be written to a part file -
   * {@link #FILE_NAME_PREFIX}.{@link #part}
   */
  public static class FileWriter extends AbstractFileOutputOperator<KeyValPair<String, Integer>>
  {
    static final String FILE_NAME_PREFIX = "filestore";

    private int part;
    private transient String currentFileName;

    private transient boolean receivedTuples;

    @Override
    public void setup(Context.OperatorContext context)
    {
      currentFileName = (part == 0) ? FILE_NAME_PREFIX : FILE_NAME_PREFIX + "." + part;
      super.setup(context);
    }

    @Override
    protected String getFileName(KeyValPair<String, Integer> keyValPair)
    {
      return currentFileName;
    }

    @Override
    protected byte[] getBytesForTuple(KeyValPair<String, Integer> keyValPair)
    {
      return (keyValPair.toString() + "\n").getBytes();
    }

    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      receivedTuples = false;
    }

    @Override
    protected void processTuple(KeyValPair<String, Integer> tuple)
    {
      super.processTuple(tuple);
      receivedTuples = true;
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
      //request for finalization if there is no input. This is done automatically if the file is rotated periodically
      // or has a size threshold.
      if (!receivedTuples && !endOffsets.isEmpty()) {
        requestFinalize(currentFileName);
        part++;
        currentFileName = FILE_NAME_PREFIX + "." + part;
      }
    }
  }
}
