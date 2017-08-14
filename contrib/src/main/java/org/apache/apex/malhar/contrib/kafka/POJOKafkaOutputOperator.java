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
package org.apache.apex.malhar.contrib.kafka;

import java.lang.reflect.Field;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.commons.lang3.ClassUtils;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * POJOKafkaOutputOperator extends from AbstractKafkaOutputOperator receives the POJO
 * from upstream and converts to Kafka Messages and writes to kafka topic.&nbsp;
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Have only one input port<br>
 * <b>Output</b>: No Output Port <br>
 * <br>
 * Properties:<br>
 * <b>brokerList</b>: List of brokers in the form of Host1:Port1,Host2:Port2,Host3:port3,...<br>
 * <b>keyField</b>: Specifies the field creates distribution of tuples to kafka partition. <br>
 * <b>isBatchProcessing</b>: Specifies whether to write messages in batch or not. By default,
 *                           the value is true <br>
 * <b>batchSize</b>: Specifies the batch size.<br>
 * <br>
 * <br>
 * </p>
 *
 * @displayName POJO Kafka Output
 * @category Messaging
 * @tags Output operator
 *
 *
 * @since 3.3.0
 */
public class POJOKafkaOutputOperator extends AbstractKafkaOutputOperator<Object,Object>
{
  @AutoMetric
  private long outputMessagesPerSec;
  @AutoMetric
  private long outputBytesPerSec;
  protected final String BROKER_KEY = "metadata.broker.list";
  protected final String BATCH_NUM_KEY = "batch.num.messages";
  protected final String PRODUCER_KEY = "producer.type";
  protected final String QUEUE_BUFFER_KEY = "queue.buffering.max.ms";
  protected final String ASYNC_PRODUCER_TYPE = "async";
  private long messageCount;
  private long byteCount;
  private String brokerList;
  private double windowTimeSec;
  private String keyField = "";
  protected boolean isBatchProcessing = true;
  @Min(2)
  protected int batchSize;
  protected transient PojoUtils.Getter keyMethod;
  protected transient Class<?> pojoClass;

  public final transient DefaultInputPort<Object> inputPort = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      if (context.getAttributes().contains(Context.PortContext.TUPLE_CLASS)) {
        pojoClass = context.getAttributes().get(Context.PortContext.TUPLE_CLASS);
      }
    }

    @Override
    public void process(Object tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * setup producer configuration.
   * @return ProducerConfig
   */
  @Override
  protected ProducerConfig createKafkaProducerConfig()
  {
    if (brokerList != null) {
      getConfigProperties().setProperty(BROKER_KEY, brokerList);
    }
    if (isBatchProcessing) {
      if (batchSize != 0) {
        getConfigProperties().setProperty(BATCH_NUM_KEY, String.valueOf(batchSize));
      }
      getConfigProperties().setProperty(PRODUCER_KEY, ASYNC_PRODUCER_TYPE);
    }
    return super.createKafkaProducerConfig();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (isBatchProcessing) {
      getConfigProperties().setProperty(QUEUE_BUFFER_KEY, String.valueOf(context.getValue(
          Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS)));
    }
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)
      * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
    if (pojoClass != null && keyField != "") {
      try {
        keyMethod = generateGetterForKeyField();
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Field " + keyField + " is invalid: " + e);
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    outputMessagesPerSec = 0;
    outputBytesPerSec = 0;
    messageCount = 0;
    byteCount = 0;
  }

  /**
   * Write the incoming tuple to Kafka
   * @param tuple incoming tuple
   */
  protected void processTuple(Object tuple)
  {
    // Get the getter method from the keyField
    if (keyMethod == null && keyField != "") {
      pojoClass = tuple.getClass();
      try {
        keyMethod = generateGetterForKeyField();
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Field " + keyField + " is invalid: " + e);
      }
    }

    // Convert the given tuple to KeyedMessage
    KeyedMessage msg;
    if (keyMethod != null) {
      msg = new KeyedMessage(getTopic(), keyMethod.get(tuple), tuple);
    } else {
      msg = new KeyedMessage(getTopic(), tuple, tuple);
    }

    getProducer().send(msg);
    messageCount++;
    if (tuple instanceof byte[]) {
      byteCount += ((byte[])tuple).length;
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    outputBytesPerSec = (long)(byteCount / windowTimeSec);
    outputMessagesPerSec = (long)(messageCount / windowTimeSec);
  }

  private PojoUtils.Getter generateGetterForKeyField() throws NoSuchFieldException, SecurityException
  {
    Field f = pojoClass.getDeclaredField(keyField);
    Class c = ClassUtils.primitiveToWrapper(f.getType());
    PojoUtils.Getter classGetter = PojoUtils.createGetter(pojoClass, keyField, c);
    return classGetter;
  }

  /**
   * Returns the broker list of kafka clusters
   * @return the broker list
   */
  public String getBrokerList()
  {
    return brokerList;
  }

  /**
   * Sets the broker list with the given list
   * @param brokerList
   */
  public void setBrokerList(@NotNull String brokerList)
  {
    this.brokerList = brokerList;
  }

  /**
   * Specifies whether want to write in batch or not.
   * @return isBatchProcessing
   */
  public boolean isBatchProcessing()
  {
    return isBatchProcessing;
  }

  /**
   * Specifies whether want to write in batch or not.
   * @param batchProcessing given batchProcessing
   */
  public void setBatchProcessing(boolean batchProcessing)
  {
    isBatchProcessing = batchProcessing;
  }

  /**
   * Returns the batch size
   * @return batch size
   */
  public int getBatchSize()
  {
    return batchSize;
  }

  /**
   * Sets the batch size
   * @param batchSize batch size
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  /**
   * Returns the key field
   * @return the key field
   */
  public String getKeyField()
  {
    return keyField;
  }

  /**
   * Sets the key field which specifies the messages writes to Kafka based on this key.
   * @param keyField the key field
   */
  public void setKeyField(String keyField)
  {
    this.keyField = keyField;
  }
}
