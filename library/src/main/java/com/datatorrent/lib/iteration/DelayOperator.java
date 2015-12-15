package com.datatorrent.lib.iteration;

import java.io.IOException;
import java.util.ArrayList;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.util.WindowDataManager;
import com.datatorrent.netlet.util.DTThrowable;

public class DelayOperator<T> implements Operator.DelayOperator, Operator.CheckpointListener
{
  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }

  private WindowDataManager windowDataManager;
  private transient long currentWindowId;
  private transient int operatorContextId;
  private transient ArrayList<T> windowData;
  private transient boolean timeToStoreTheWindow = false;

  public transient DefaultInputPort<T> input = new DefaultInputPort<T>() {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }
  };

  public transient DefaultOutputPort<T> output = new DefaultOutputPort();

  DelayOperator()
  {
    windowData = new ArrayList<>();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.operatorContextId = context.getId();
    this.windowDataManager.setup(context);

    if ( context.getWindowsFromCheckpoint() == 1 ) {
      timeToStoreTheWindow = true;
    }
  }

  @Override
  public void teardown()
  {
    this.windowDataManager.teardown();
  }

  @Override
  public void firstWindow(long windowId)
  {
    replay(windowId);
  }

  private void replay( long windowId )
  {
    ArrayList<T> recoveredData;
    try {
      recoveredData = (ArrayList<T>)this.windowDataManager.load(operatorContextId, windowId);
      if (recoveredData == null) {
        return;
      }
      for ( T tuple : recoveredData) {
        processTuple(tuple);
      }
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
  }

  @Override
  public void endWindow()
  {
    if (timeToStoreTheWindow) {
      try {
        this.windowDataManager.save(windowData, operatorContextId, currentWindowId);
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }

      windowData.clear();
    }
  }

  protected void processTuple(T tuple)
  {
    output.emit(tuple);

    if ( timeToStoreTheWindow ) {
      windowData.add(tuple);
    }
  }

  @Override
  public void checkpointed(long l)
  {

  }

  @Override
  public void committed(long windowId)
  {
    try {
      windowDataManager.deleteUpTo(operatorContextId, windowId);
    } catch (IOException e) {
      throw new RuntimeException("committing", e);
    }
  }
}

