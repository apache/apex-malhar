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
package org.apache.apex.benchmark.hive;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.hive.AbstractFSRollingOutputOperator;
import org.apache.apex.malhar.hive.HiveOperator;
import org.apache.apex.malhar.hive.HiveStore;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

import com.datatorrent.api.StreamingApplication;

import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Application used to benchmark HIVE Insert operator
 * The DAG consists of random word generator operator that is
 * connected to Hive output operator that writes to a Hive table partition using file written in HDFS.&nbsp;
 * The file contents are being written by the word generator.
 * BenchMark Results For RollingFsWriter
 * -------------------------------------
 * The operator writes 21 MB/s with the following configuration
 *
 * Container memory size=4G
 * Tuple byte size of 32
 * output to a single file
 * Application window length of 1 second
 *
 * The operator writes 25 MB/s with the following configuration
 *
 * Container memory size=4G
 * Tuple byte size of 32
 * output to a single file
 * Application window length of 30 seconds
 *
 * BenchMark Results for HiveOperator
 * -----------------------------------
 * This operator loads 187 files in 120 seconds with following configuration.
 * Tuple byte size of 32, file size 512.
 *
 * @since 2.1.0
 */
@ApplicationAnnotation(name = "HiveInsertBenchmarkingApp")
public class HiveInsertBenchmarkingApp implements StreamingApplication
{
  Logger LOG = LoggerFactory.getLogger(HiveInsertBenchmarkingApp.class);

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HiveStore store = new HiveStore();
    store.setDatabaseUrl(conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.store.dbUrl"));
    store.setConnectionProperties(conf.get(
        "dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.store.connectionProperties"));
    store.setFilepath(conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.store.filepath"));

    try {
      hiveInitializeDatabase(store, conf.get(
          "dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.tablename"));
    } catch (SQLException ex) {
      LOG.debug(ex.getMessage());
    }

    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);
    RandomDateGenerator dateGenerator = dag.addOperator("DateGenerator", new RandomDateGenerator());
    FSRollingTestImpl rollingFsWriter = dag.addOperator("RollingFsWriter", new FSRollingTestImpl());
    rollingFsWriter.setFilePath(store.filepath);
    HiveOperator hiveInsert = dag.addOperator("HiveOperator", new HiveOperator());
    hiveInsert.setStore(store);
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hiveInsert.setHivePartitionColumns(hivePartitionColumns);
    dag.addStream("Generator2HDFS", dateGenerator.outputString, rollingFsWriter.input);
    dag.addStream("FsWriter2Hive", rollingFsWriter.outputPort, hiveInsert.input);
  }

  /*
   * User can create table and specify data columns and partition columns in this function.
   */
  public static void hiveInitializeDatabase(HiveStore hiveStore, String tablename) throws SQLException
  {
    hiveStore.connect();
    Statement stmt = hiveStore.getConnection().createStatement();
    stmt.execute("CREATE TABLE IF NOT EXISTS " + tablename
        + " (col1 string) PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
        + "STORED AS TEXTFILE ");
    hiveStore.disconnect();
  }

  private static class FSRollingTestImpl extends AbstractFSRollingOutputOperator<String>
  {
    @Override
    public ArrayList<String> getHivePartition(String tuple)
    {
      ArrayList<String> hivePartitions = new ArrayList<String>();
      hivePartitions.add(tuple);
      return (hivePartitions);
    }

    @Override
    protected byte[] getBytesForTuple(String tuple)
    {
      return (tuple + "\n").getBytes();
    }

  }

  public static class RandomDateGenerator implements InputOperator
  {
    /**
     * A counter which is used to emit the same number of tuples per
     * application window.
     */
    private int tupleCounter = 0;
    public final transient DefaultOutputPort<String> outputString = new DefaultOutputPort<String>();
    /**
     * The default number of tuples emitted per window.
     */
    public static final int MAX_TUPLES_PER_WINDOW = 100;

    /**
     * The number of tuples per window.
     */
    @Min(1)
    private int tuplesPerWindow = MAX_TUPLES_PER_WINDOW;
    /**
     * The random object use to generate the tuples.
     */
    private transient Random random = new Random();

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void beginWindow(long windowId)
    {
      tupleCounter = 0;
    }

    @Override
    public void emitTuples()
    {

      for (;
          tupleCounter < tuplesPerWindow;
          tupleCounter++) {
        String output = "2014-12-1" + random.nextInt(10) + "";
        outputString.emit(output);
      }

    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void teardown()
    {
    }

    /**
     * Sets the number of tuples emitted per application window.
     *
     * @param tuplesPerWindow The number of tuples emitted per application window.
     */
    public void setTuplesPerWindow(int tuplesPerWindow)
    {
      this.tuplesPerWindow = tuplesPerWindow;
    }

    /**
     * Gets the number of tuples emitted per application window.
     *
     * @return The number of tuples emitted per application window.
     */
    public int getTuplesPerWindow()
    {
      return tuplesPerWindow;
    }

  }
}
