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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.apex.malhar.kafka.AbstractKafkaConsumer;
import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.apex.malhar.kafka.KafkaConsumer09;
import org.apache.apex.malhar.lib.algo.UniqueCounter;
import org.apache.apex.malhar.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;
import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "ExactlyOnceJbdcOutput")
/**
 * @since 3.8.0
 */
public class ExactlyOnceJdbcOutputApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput",
        new KafkaSinglePortStringInputOperator());
    kafkaInput.setWindowDataManager(new FSWindowDataManager());
    UniqueCounterFlat count = dag.addOperator("count", new UniqueCounterFlat());
    CountStoreOperator store = dag.addOperator("store", new CountStoreOperator());
    store.setStore(new JdbcTransactionalStore());
    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("words", kafkaInput.outputPort, count.data);
    dag.addStream("counts", count.counts, store.input, cons.input);
  }

  public static class CountStoreOperator
      extends AbstractJdbcTransactionableOutputOperator<KeyValPair<String, Integer>>
  {
    public static final String SQL =
        "MERGE INTO words USING (VALUES ?, ?) I (word, wcount)"
        + " ON (words.word=I.word)"
        + " WHEN MATCHED THEN UPDATE SET words.wcount = words.wcount + I.wcount"
        + " WHEN NOT MATCHED THEN INSERT (word, wcount) VALUES (I.word, I.wcount)";

    @Override
    protected String getUpdateCommand()
    {
      return SQL;
    }

    @Override
    protected void setStatementParameters(PreparedStatement statement,
        KeyValPair<String, Integer> tuple) throws SQLException
    {
      statement.setString(1, tuple.getKey());
      statement.setInt(2, tuple.getValue());
    }
  }

  /**
   * Extension of {@link UniqueCounter} that emits individual key/value pairs instead
   * of map with all modified values.
   */
  public static class UniqueCounterFlat extends UniqueCounter<String>
  {
    public final transient DefaultOutputPort<KeyValPair<String, Integer>> counts = new DefaultOutputPort<>();

    @Override
    public void endWindow()
    {
      for (Map.Entry<String, MutableInt> e: map.entrySet()) {
        counts.emit(new KeyValPair<>(e.getKey(), e.getValue().toInteger()));
      }
      map.clear();
    }
  }

  public static class KafkaSinglePortStringInputOperator extends AbstractKafkaInputOperator
  {
    public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();

    @Override
    public AbstractKafkaConsumer createConsumer(Properties properties)
    {
      return new KafkaConsumer09(properties);
    }

    @Override
    protected void emitTuple(String cluster, ConsumerRecord<byte[], byte[]> message)
    {
      outputPort.emit(new String(message.value()));
    }
  }

}
