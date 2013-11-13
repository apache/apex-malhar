/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 *<br>Base output adapter which processes tuples that were generated after the last persisted WindowID.</br>
 *<br>The operator collects the tuples that arrive in a window and stores them in the database in the endWindow.
 * The tuples are stored in an application specific manner. The concrete implementation that extends this operator
 * provides a method that specifies where to store the tuples.</br>
 * <br></br>
 * <br>It also stores the last processed window id in the database and loads it during setup time.
 * If the processing window id is not greater than the last processed window id that was loaded then the tuples
 * are ignored till the processing window id becomes greater than the last processed window id. </br>
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
    applicationName =  context.getValue(DAG.APPLICATION_NAME);
    applicationId = context.getValue(DAG.APPLICATION_ID);
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
}
