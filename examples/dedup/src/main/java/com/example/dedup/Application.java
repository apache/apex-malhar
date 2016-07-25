/**
 * Put your copyright and license info here.
 */
package com.example.dedup;

import java.util.Date;
import java.util.Random;

import org.apache.apex.malhar.lib.dedup.TimeBasedDedupOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

@ApplicationAnnotation(name="DedupExample")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Test Data Generator Operator
    RandomDataGeneratorOperator gen = dag.addOperator("RandomGenerator", new RandomDataGeneratorOperator());

    // Dedup Operator. Configuration through resources/META-INF/properties.xml
    TimeBasedDedupOperator dedup = dag.addOperator("Deduper", new TimeBasedDedupOperator());

    // Console output operator for unique tuples
    ConsoleOutputOperator consoleUnique = dag.addOperator("ConsoleUnique", new ConsoleOutputOperator());

    // Console output operator for duplicate tuples
    ConsoleOutputOperator consoleDuplicate = dag.addOperator("ConsoleDuplicate", new ConsoleOutputOperator());

    // Console output operator for duplicate tuples
    ConsoleOutputOperator consoleExpired = dag.addOperator("ConsoleExpired", new ConsoleOutputOperator());

    // Streams
    dag.addStream("Generator to Dedup", gen.output, dedup.input);

    // Connect Dedup unique to Console
    dag.addStream("Dedup Unique to Console", dedup.unique, consoleUnique.input);
    // Connect Dedup duplicate to Console
    dag.addStream("Dedup Duplicate to Console", dedup.duplicate, consoleDuplicate.input);
    // Connect Dedup expired to Console
    dag.addStream("Dedup Expired to Console", dedup.expired, consoleExpired.input);

    // Set Attribute TUPLE_CLASS for supplying schema information to the port
    dag.setInputPortAttribute(dedup.input, Context.PortContext.TUPLE_CLASS, TestEvent.class);

    // Uncomment the following line to create multiple partitions for Dedup operator. In this case: 2
    // dag.setAttribute(dedup, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<TimeBasedDedupOperator>(2));
  }

  public static class RandomDataGeneratorOperator extends BaseOperator implements InputOperator
  {

    public final transient DefaultOutputPort<TestEvent> output = new DefaultOutputPort<>();
    private final transient Random r = new Random();
    private int tuplesPerWindow = 100;
    private transient int count = 0;

    @Override
    public void beginWindow(long windowId) {
      count = 0;
    }

    @Override
    public void emitTuples()
    {
      if (count++ > tuplesPerWindow) {
        return;
      }
      TestEvent event = new TestEvent();
      event.id = r.nextInt(100);
      event.eventTime = new Date(System.currentTimeMillis() - (r.nextInt(60 * 1000)));
      output.emit(event);
    }
  }

  public static class TestEvent
  {
    private int id;
    private Date eventTime;

    public TestEvent()
    {
    }

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

    public Date getEventTime()
    {
      return eventTime;
    }

    public void setEventTime(Date eventTime)
    {
      this.eventTime = eventTime;
    }

    @Override
    public String toString() {
      return "TestEvent [id=" + id + ", eventTime=" + eventTime + "]";
    }

  }

}
