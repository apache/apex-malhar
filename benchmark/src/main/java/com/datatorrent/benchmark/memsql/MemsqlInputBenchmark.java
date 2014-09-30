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

package com.datatorrent.benchmark.memsql;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.memsql.*;
import static com.datatorrent.contrib.memsql.AbstractMemsqlOutputOperatorTest.*;
import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.*;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.stream.DevNull;
import java.util.Random;
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

  private static final long SEED_SIZE = 10000;

  public String host = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    MemsqlStore memsqlStore = new MemsqlStore();
    memsqlStore.setDbUrl(conf.get("rootDbUrl"));
    memsqlStore.setConnectionProperties(conf.get("dt.application.MemsqlInputBenchmark.operator.memsqlInputOperator.store.connectionProperties"));

    AbstractMemsqlOutputOperatorTest.memsqlInitializeDatabase(memsqlStore);

    memsqlStore.setDbUrl(conf.get("dt.application.MemsqlInputBenchmark.operator.memsqlInputOperator.store.dbUrl"));

    MemsqlOutputOperator outputOperator = new MemsqlOutputOperator();
    outputOperator.setStore(memsqlStore);
    outputOperator.setBatchSize(BATCH_SIZE);

    Random random = new Random();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    long seedSize = conf.getLong("seedSize", SEED_SIZE);

    outputOperator.setup(context);
    outputOperator.beginWindow(0);

    for(long valueCounter = 0;
        valueCounter < seedSize;
        valueCounter++) {
      outputOperator.input.put(random.nextInt());
    }

    outputOperator.endWindow();
    outputOperator.teardown();


    MemsqlInputOperator memsqlInputOperator = dag.addOperator("memsqlInputOperator",
                                                                new MemsqlInputOperator());

    DevNull<Integer> devNull = dag.addOperator("devnull",
                                      new DevNull<Integer>());

    dag.addStream("memsqlconnector",
                  memsqlInputOperator.outputPort,
                  devNull.data);
  }
}
