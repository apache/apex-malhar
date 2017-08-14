/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.exactlyonce;

import org.apache.apex.examples.exactlyonce.ExactlyOnceJdbcOutputApp.KafkaSinglePortStringInputOperator;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.io.fs.AbstractFileOutputOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "ExactlyOnceFileOutput")
/**
 * @since 3.8.0
 */
public class ExactlyOnceFileOutputApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput",
        new KafkaSinglePortStringInputOperator());
    kafkaInput.setWindowDataManager(new FSWindowDataManager());

    ExactlyOnceJdbcOutputApp.UniqueCounterFlat count = dag.addOperator("count",
        new ExactlyOnceJdbcOutputApp.UniqueCounterFlat());

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
