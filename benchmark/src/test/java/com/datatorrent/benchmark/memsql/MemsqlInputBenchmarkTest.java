/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.benchmark.memsql;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.contrib.memsql.*;
import static com.datatorrent.contrib.memsql.AbstractMemsqlOutputOperatorTest.BATCH_SIZE;
import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.*;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemsqlInputBenchmarkTest
{
  private static final Logger LOG = LoggerFactory.getLogger(MemsqlInputBenchmarkTest.class);
  private static final long SEED_SIZE = 10000;

  @Test
  public void testMethod() throws SQLException, IOException
  {
    Configuration conf = new Configuration();
    InputStream inputStream = new FileInputStream("src/site/conf/dt-site-memsql.xml");
    conf.addResource(inputStream);

    MemsqlStore memsqlStore = new MemsqlStore();
    memsqlStore.setDatabaseUrl(conf.get("dt.rootDbUrl"));
    memsqlStore.setConnectionProperties(conf.get("dt.application.MemsqlInputBenchmark.operator.memsqlInputOperator.store.connectionProperties"));

    AbstractMemsqlOutputOperatorTest.memsqlInitializeDatabase(memsqlStore);

    MemsqlPOJOOutputOperator outputOperator = new MemsqlPOJOOutputOperator();
    outputOperator.getStore().setDatabaseUrl(conf.get("dt.application.MemsqlInputBenchmark.operator.memsqlInputOperator.store.dbUrl"));
    outputOperator.getStore().setConnectionProperties(conf.get("dt.application.MemsqlInputBenchmark.operator.memsqlInputOperator.store.connectionProperties"));
    outputOperator.setBatchSize(BATCH_SIZE);

    Random random = new Random();
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    long seedSize = conf.getLong("dt.seedSize", SEED_SIZE);

    outputOperator.setup(context);
    outputOperator.beginWindow(0);

    for(long valueCounter = 0;
        valueCounter < seedSize;
        valueCounter++) {
      outputOperator.input.put(random.nextInt());
    }

    outputOperator.endWindow();
    outputOperator.teardown();

    MemsqlInputBenchmark app = new MemsqlInputBenchmark();
    LocalMode lm = LocalMode.newInstance();

    try {
      lm.prepareDAG(app, conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(20000);
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }

    IOUtils.closeQuietly(inputStream);
  }
}
