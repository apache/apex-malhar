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

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.memsql.*;
import com.datatorrent.lib.stream.DevNull;
import java.sql.*;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BenchMark Results
 * -----------------
 * The operator operates at 450,000 tuples/sec with the following configuration
 *
 * Container memory size=1G
 * Default memsql configuration
 * memsql number of write threads=1
 * batch size 1000
 * 1 master aggregator, 1 leaf
 *
 * @since 1.0.5
 */
@ApplicationAnnotation(name="MemsqlInputBenchmark")
public class MemsqlInputBenchmark implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(MemsqlInputBenchmark.class);
  private static final int BLAST_SIZE = 10000;
  private static final Locality LOCALITY = null;

  public String host = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    if(host != null) {
      conf.set(this.getClass().getName() + ".host", host);
    }

    MemsqlStore memsqlStore = AbstractMemsqlOutputOperatorTest.createStore(conf, this.getClass().getName(), true);

    memsqlStore.connect();

    boolean hasTestDB = false;

    try {
      ResultSet resultSet = memsqlStore.getConnection().getMetaData().getCatalogs();

      while(resultSet.next())
      {
        String databaseName = resultSet.getString(1);
        if(databaseName.equals(AbstractMemsqlOutputOperatorTest.DATABASE)) {
          hasTestDB = true;
        }
      }
    }
    catch(SQLException ex)
    {
      LOG.error("Error while checking database.", ex);
      return;
    }

    if(!hasTestDB) {
      LOG.error("There is no test database to run off of. " +
                "Please run MemsqlOutputBenchmark first before running this.");
    }

    memsqlStore.disconnect();


    MemsqlInputOperator memsqlInputOperator = dag.addOperator("memsqlInputOperator",
                                                                new MemsqlInputOperator());
    memsqlInputOperator.setBlastSize(BLAST_SIZE);

    memsqlInputOperator.setStore(memsqlStore);

    DevNull<Integer> devNull = dag.addOperator("devnull",
                                      new DevNull<Integer>());

    dag.addStream("memsqlconnector",
                  memsqlInputOperator.outputPort,
                  devNull.data).setLocality(LOCALITY);
  }
}
