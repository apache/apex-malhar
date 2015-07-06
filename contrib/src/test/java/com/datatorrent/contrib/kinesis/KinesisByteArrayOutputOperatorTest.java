package com.datatorrent.contrib.kinesis;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.model.Record;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.Pair;
import com.datatorrent.contrib.util.FieldInfo;
import com.datatorrent.contrib.util.FieldValueSerializableGenerator;
import com.datatorrent.contrib.util.POJOTupleGenerateOperator;
import com.datatorrent.contrib.util.TestPOJO;
import com.datatorrent.contrib.util.TupleGenerator;

@SuppressWarnings("rawtypes")
public class KinesisByteArrayOutputOperatorTest extends KinesisOutputOperatorTest<KinesisByteArrayOutputOperator, POJOTupleGenerateOperator>
{
  public static class TestPOJOTupleGenerateOperator extends POJOTupleGenerateOperator<TestPOJO>
  {
    public TestPOJOTupleGenerateOperator()
    {
      super(TestPOJO.class);
      setTupleNum(maxTuple);
    }
  }

  private FieldValueSerializableGenerator fieldValueGenerator;

  @Test
  public void testKinesisOutputOperatorInternal() throws Exception
  {
    KinesisByteArrayOutputOperator operator = new KinesisByteArrayOutputOperator();
    configureTestingOperator(operator);
    operator.setBatchProcessing(false);

    operator.setup(null);

    TupleGenerator<TestPOJO> generator = new TupleGenerator<TestPOJO>(TestPOJO.class);

    // read tuples
    KinesisTestConsumer listener = createConsumerListener(streamName);
    String iterator = listener.prepareIterator();
    // save the tuples
    for (int i = 0; i < maxTuple; ++i) {
      if (i % 2 == 0)
        iterator = listener.processNextIterator(iterator);

      operator.processTuple(getNextTuple(generator));
    }
    iterator = listener.processNextIterator(iterator);
  }

  protected Pair<String, byte[]> getNextTuple(TupleGenerator<TestPOJO> generator)
  {
    TestPOJO obj = generator.getNextTuple();
    if (fieldValueGenerator == null)
      fieldValueGenerator = FieldValueSerializableGenerator.getFieldValueGenerator(TestPOJO.class, null);
    return new Pair<String, byte[]>(obj.getRow(), fieldValueGenerator.serializeObject(obj));
  }

  @Override
  protected POJOTupleGenerateOperator addGenerateOperator(DAG dag)
  {
    return dag.addOperator("TestPojoGenerator", TestPOJOTupleGenerateOperator.class);
  }

  @Override
  protected DefaultOutputPort getOutputPortOfGenerator(POJOTupleGenerateOperator generator)
  {
    return generator.outputPort;
  }

  @Override
  protected KinesisByteArrayOutputOperator addTestingOperator(DAG dag)
  {
    KinesisByteArrayOutputOperator operator = dag.addOperator("Test-KinesisByteArrayOutputOperator", KinesisByteArrayOutputOperator.class);

    operator.setBatchProcessing(true);

    return operator;
  }

  @Override
  protected KinesisTestConsumer createConsumerListener(String streamName)
  {
    KinesisEmployeeConsumer listener = new KinesisEmployeeConsumer(streamName);
    return listener;
  }

  public static class KinesisEmployeeConsumer extends KinesisTestConsumer
  {
    private static final Logger logger = LoggerFactory.getLogger(KinesisEmployeeConsumer.class);
    protected FieldValueSerializableGenerator<FieldInfo> fieldValueGenerator = FieldValueSerializableGenerator.getFieldValueGenerator(TestPOJO.class, null);

    public KinesisEmployeeConsumer(String streamNamem)
    {
      super(streamNamem);
    }

    @Override
    protected void processRecord(Record record)
    {
      String partitionKey = record.getPartitionKey();
      ByteBuffer data = record.getData();
      logger.info("partitionKey={} ", partitionKey);
      byte[] dataBytes = new byte[data.remaining()];
      data.get(dataBytes, 0, dataBytes.length);

      long key = Long.valueOf(partitionKey);
      TestPOJO expected = new TestPOJO(key);

      TestPOJO read = (TestPOJO) fieldValueGenerator.deserializeObject(dataBytes);

      if (!read.outputFieldsEquals(expected)) {
        logger.error("read is not same as expected. read={}, expected={}", read, expected);
        Assert.assertTrue(false);
      } else {
        logger.info("read is same as expected. read={}, expected={}", read, expected);
      }
    }
  }
}
