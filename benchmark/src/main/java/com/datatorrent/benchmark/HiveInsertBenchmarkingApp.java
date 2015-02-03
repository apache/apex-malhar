/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.benchmark;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.testbench.RandomWordGenerator;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hive.*;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application used to benchmark HIVE Insert operator
 * The DAG consists of random word generator operator that is
 * connected to Hive output operator that writes to a Hive table partition using file written in HDFS.&nbsp;
 * The file contents are being written by the word generator.
 * <p>
 *
 */
@ApplicationAnnotation(name = "HiveInsertBenchmarkingApp")
public class HiveInsertBenchmarkingApp implements StreamingApplication
{
  Logger LOG = LoggerFactory.getLogger(HiveInsertBenchmarkingApp.class);

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HiveStore store = new HiveStore();
    store.setDbUrl(conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.store.dbUrl"));
    store.setConnectionProperties(conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.store.connectionProperties"));
    store.setFilepath(conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.store.filepath"));

    try {
      hiveInitializeDatabase(store, conf.get("dt.application.HiveInsertBenchmarkingApp.operator.HiveOperator.tablename"));
    }
    catch (SQLException ex) {
      LOG.debug(ex.getMessage());
    }

    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);
    RandomStringGenerator wordGenerator = dag.addOperator("WordGenerator", RandomStringGenerator.class);
    dag.setAttribute(wordGenerator, PortContext.QUEUE_CAPACITY, 10000);
    FSRollingTestImpl rollingFsWriter = dag.addOperator("RollingFsWriter", new FSRollingTestImpl());
    rollingFsWriter.setFilePath(store.filepath);
    rollingFsWriter.setFilePermission(Integer.parseInt(conf.get("dt.application.HiveInsertBenchmarkingApp.operator.RollingFsWriter.filePermission")));
    rollingFsWriter.setConverter(new StringConverter());

    HiveOperator hiveInsert = dag.addOperator("HiveOperator", new HiveOperator());
    hiveInsert.setStore(store);
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hiveInsert.setHivePartitionColumns(hivePartitionColumns);
    dag.addStream("Generator2HDFS", wordGenerator.outputString, rollingFsWriter.input);
    dag.addStream("FsWriter2Hive", rollingFsWriter.outputPort, hiveInsert.input);
  }

  /*
   * User can create table and specify data columns and partition columns in this function.
   */
  public static void hiveInitializeDatabase(HiveStore hiveStore, String tablename) throws SQLException
  {
    hiveStore.connect();
    Statement stmt = hiveStore.getConnection().createStatement();
    stmt.execute("CREATE TABLE IF NOT EXISTS " + tablename + " (col1 string) PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
            + "STORED AS TEXTFILE ");
    hiveStore.disconnect();
  }

  private static class FSRollingTestImpl extends FSRollingOutputOperator<String>
  {
   private final Random random = new Random();

    @Override
    public ArrayList<String> getHivePartition(String tuple)
    {
      ArrayList<String> hivePartitions = new ArrayList<String>();
      hivePartitions.add("2014-12-1"+ random.nextInt(10));
      return (hivePartitions);
    }

  }

}
