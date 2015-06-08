package com.datatorrent.contrib.testhelper;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;

public class SourceModule extends BaseOperator
implements InputOperator, ActivationListener<OperatorContext>
{
  public final transient DefaultOutputPort<byte[]> outPort = new DefaultOutputPort<byte[]>();
  transient ArrayBlockingQueue<byte[]> holdingBuffer;
  int testNum;

  private static org.slf4j.Logger logger;

  public SourceModule()
  {
    logger =  LoggerFactory.getLogger(SourceModule.class);;
  }

  public SourceModule(org.slf4j.Logger loggerInstance)
  {
    logger = loggerInstance;
  }

  @Override
  public void setup(OperatorContext context)
  {
    holdingBuffer = new ArrayBlockingQueue<byte[]>(1024 * 1024);
  }

  public void emitTuple(byte[] message)
  {
    logger.debug("Emmiting message " + message.toString());
    outPort.emit(message);
  }

  @Override
  public void emitTuples()
  {
    for (int i = holdingBuffer.size(); i-- > 0;) {
      emitTuple(holdingBuffer.poll());
    }
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    for (int i = 0; i < testNum; i++) {
      HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
      dataMapa.put("a", 2);
      holdingBuffer.add(dataMapa.toString().getBytes());

      HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
      dataMapb.put("b", 20);
      holdingBuffer.add(dataMapb.toString().getBytes());

      HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
      dataMapc.put("c", 1000);
      holdingBuffer.add(dataMapc.toString().getBytes());
    }
  }

  public void setTestNum(int testNum)
  {
    this.testNum = testNum;
  }

  @Override
  public void deactivate()
  {
  }

  public void replayTuples(long windowId)
  {
  }
}


class SourceModule1 extends BaseOperator
implements InputOperator, ActivationListener<OperatorContext>
{
  public final transient DefaultOutputPort<byte[]> outPort = new DefaultOutputPort<byte[]>();
  transient ArrayBlockingQueue<byte[]> holdingBuffer;
  int testNum;

  @Override
  public void setup(OperatorContext context)
  {
    holdingBuffer = new ArrayBlockingQueue<byte[]>(1024 * 1024);
  }

  public void emitTuple(byte[] message)
  {
    outPort.emit(message);
  }

  @Override
  public void emitTuples()
  {
    for (int i = holdingBuffer.size(); i-- > 0;) {
      emitTuple(holdingBuffer.poll());
    }
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    for (int i = 0; i < testNum; i++) {
      HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
      dataMapa.put("a", 2);
      holdingBuffer.add(dataMapa.toString().getBytes());

      HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
      dataMapb.put("b", 20);
      holdingBuffer.add(dataMapb.toString().getBytes());

      HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
      dataMapc.put("c", 1000);
      holdingBuffer.add(dataMapc.toString().getBytes());
    }
  }

  public void setTestNum(int testNum)
  {
    this.testNum = testNum;
  }

  @Override
  public void deactivate()
  {
  }

  public void replayTuples(long windowId)
  {
  }
}
