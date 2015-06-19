package com.datatorrent.contrib.kinesis;

import java.util.concurrent.ArrayBlockingQueue;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;

public class KinesisStringOutputOperatorTest extends KinesisOutputOperatorTest< KinesisStringOutputOperator, KinesisStringOutputOperatorTest.StringGeneratorInputOperator >
{
  @Override
  protected StringGeneratorInputOperator addGenerateOperator( DAG dag )
  {
    return dag.addOperator("TestStringGenerator", StringGeneratorInputOperator.class);
    //StringGeneratorInputOperator generator =
  }
  
  @Override
  protected DefaultOutputPort getOutputPortOfGenerator( StringGeneratorInputOperator generator )
  {
    return generator.outputPort;
  }
  
  @Override
  protected KinesisStringOutputOperator addTestingOperator(DAG dag)
  {
    KinesisStringOutputOperator operator = dag.addOperator("KinesisMessageProducer", KinesisStringOutputOperator.class );

    return operator;
  }

  

  /**
   * Tuple generator for testing.
   */
  public static class StringGeneratorInputOperator implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>();
    private final transient ArrayBlockingQueue<String> stringBuffer = new ArrayBlockingQueue<String>(1024);
    private volatile Thread dataGeneratorThread;

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void activate(OperatorContext ctx)
    {
      dataGeneratorThread = new Thread("String Generator")
      {
        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run()
        {
          try {
            int i = 0;
            while (dataGeneratorThread != null && i < maxTuple) {
              stringBuffer.put("testString " + (++i));
            }
          }
          catch (Exception ie) {
            throw new RuntimeException(ie);
          }
        }
      };
      dataGeneratorThread.start();
    }

    @Override
    public void deactivate()
    {
      dataGeneratorThread = null;
    }

    @Override
    public void emitTuples()
    {
      for (int i = stringBuffer.size(); i-- > 0;) {
        outputPort.emit(stringBuffer.poll());
      }
    }
  } // End of StringGeneratorInputOperator

}
