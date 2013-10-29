package com.datatorrent.contrib.couchdb;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Base output adapter which processes tuples that were generated after the last persisted WindowID<br></br>
 *
 * @param <T> Type of objects that DB operator accepts</T>
 * @since 0.3.5
 */
public abstract class AbstractDBOutputOperator<T> implements Operator
{

  private static final transient Logger LOG = LoggerFactory.getLogger(AbstractDBOutputOperator.class);

  protected transient String applicationName;
  protected transient String applicationId;
  protected transient int operatorId;

  private transient long lastPersistedWindow;
  private transient long currentWindow;
  private transient List<T> tuples;

  @InputPortFieldAnnotation(name = "inputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {

    @Override
    public void process(T tuple)
    {
      if (currentWindow > lastPersistedWindow) {
        tuples.add(tuple);
      }
    }
  };

  public AbstractDBOutputOperator()
  {
    this.lastPersistedWindow = -1;
    this.currentWindow = 0;
    this.tuples = Lists.newArrayList();
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindow = windowId;
  }

  @Override
  public void endWindow()
  {
    for (T tuple : tuples)
      storeData(tuple);
    tuples.clear();
    storeWindow(currentWindow);
    lastPersistedWindow = currentWindow;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    applicationName = context.attrValue(DAG.APPLICATION_NAME, getDefaultApplicationName());
    applicationId = context.attrValue(DAG.APPLICATION_ID, "AppId");
    operatorId = context.getId();
    Long lastPersistedWindow = getLastPersistedWindow();

    if (lastPersistedWindow != null)
      this.lastPersistedWindow = lastPersistedWindow;
  }

  @Override
  public void teardown()
  {
  }

  @Nullable
  public abstract Long getLastPersistedWindow();

  public abstract void storeData(T tuple);

  public abstract void storeWindow(long windowId);

  public abstract String getDefaultApplicationName();
}
