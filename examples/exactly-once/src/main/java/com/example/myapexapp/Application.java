/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.util.BaseUniqueKeyCounter;
import com.datatorrent.lib.util.KeyValPair;

@ApplicationAnnotation(name="ExactlyOnceExampleApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput", new KafkaSinglePortStringInputOperator());
    kafkaInput.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());
    UniqueCounter<String> count = dag.addOperator("count", new UniqueCounter<String>());
    CountStoreOperator store = dag.addOperator("store", new CountStoreOperator());
    store.setStore(new JdbcTransactionalStore());
    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("words", kafkaInput.outputPort, count.data);
    dag.addStream("counts", count.count, store.input, cons.input);
  }

  public static class CountStoreOperator extends AbstractJdbcTransactionableOutputOperator<KeyValPair<String, Integer>>
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
    protected void setStatementParameters(PreparedStatement statement, KeyValPair<String, Integer> tuple) throws SQLException
    {
      statement.setString(1, tuple.getKey());
      statement.setInt(2, tuple.getValue());
    }
  }

  public static class UniqueCounter<K> extends BaseUniqueKeyCounter<K>
  {
    /**
     * The input port which receives incoming tuples.
     */
    public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
    {
      /**
       * Reference counts tuples
       */
      @Override
      public void process(K tuple)
      {
        processTuple(tuple);
      }

    };

    public final transient DefaultOutputPort<KeyValPair<K, Integer>> count = new DefaultOutputPort<KeyValPair<K, Integer>>()
    {
      @Override
      public Unifier<KeyValPair<K, Integer>> getUnifier()
      {
        throw new UnsupportedOperationException("not partitionable");
      }
    };

    @Override
    public void endWindow()
    {
      for (Map.Entry<K, MutableInt> e: map.entrySet()) {
        count.emit(new KeyValPair<>(e.getKey(), e.getValue().toInteger()));
      }
      map.clear();
    }

  }

}
