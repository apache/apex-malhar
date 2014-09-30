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

package com.datatorrent.contrib.memsql;

import static com.datatorrent.contrib.memsql.AbstractMemsqlOutputOperatorTest.*;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AbstractMemsqlInputOperatorTest
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMemsqlInputOperatorTest.class);

  public static void populateDatabase(MemsqlStore memsqlStore)
  {
    memsqlStore.connect();

    try {
      Random random = new Random();
      Statement statement = memsqlStore.getConnection().createStatement();

      for(int counter = 0;
          counter < DATABASE_SIZE;
          counter++) {
        statement.executeUpdate("insert into " +
                                FQ_TABLE +
                                " (" + DATA_COLUMN + ") values (" + random.nextInt() + ")");
      }

      statement.close();
    }
    catch (SQLException ex) {
      LOG.error(null, ex);
    }

    memsqlStore.disconnect();
  }

  @Test
  public void TestMemsqlInputOperator()
  {
    cleanDatabase();
    populateDatabase(createStore(null, true));

    MemsqlInputOperator inputOperator = new MemsqlInputOperator();
    createStore(inputOperator.getStore(), true);
    inputOperator.setBlastSize(BLAST_SIZE);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(null);

    for(int wid = 0;
        wid < NUM_WINDOWS + 1;
        wid++) {
      inputOperator.beginWindow(wid);
      inputOperator.emitTuples();
      inputOperator.endWindow();
    }

    Assert.assertEquals("Number of tuples in database", DATABASE_SIZE, sink.collectedTuples.size());
  }
}
