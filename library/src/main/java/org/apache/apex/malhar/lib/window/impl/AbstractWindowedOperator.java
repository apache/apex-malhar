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
package org.apache.apex.malhar.lib.window.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.validation.ValidationException;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.spillable.WindowListener;
import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.ImplicitWatermarkGenerator;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.WindowedOperator;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Function;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is the abstract windowed operator class that implements most of the windowing, triggering, and accumulating
 * concepts. The subclass of this abstract class is supposed to provide the implementation of how the accumulated
 * values are stored in the storage.
 *
 * @param <InputT> The type of the input tuple
 * @param <OutputT> The type of the output tuple
 * @param <DataStorageT> The type of the data storage
 * @param <RetractionStorageT> The type of the retraction storage
 * @param <AccumulationT> The type of the accumulation
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public abstract class AbstractWindowedOperator<InputT, OutputT, DataStorageT extends WindowedStorage, RetractionStorageT extends WindowedStorage, AccumulationT extends Accumulation>
    extends BaseOperator implements WindowedOperator<InputT>, Operator.CheckpointNotificationListener
{

  protected WindowOption windowOption;
  protected TriggerOption triggerOption;
  protected long allowedLatenessMillis = -1;
  protected WindowedStorage.WindowedPlainStorage<WindowState> windowStateMap;

  private Function<InputT, Long> timestampExtractor;

  protected long nextWatermark = -1;
  protected long currentWatermark = -1;
  private boolean triggerAtWatermark;
  protected long earlyTriggerCount;
  protected long earlyTriggerMillis;
  protected long lateTriggerCount;
  protected long lateTriggerMillis;
  private long currentDerivedTimestamp = -1;
  private long timeIncrement;
  protected long fixedWatermarkMillis = -1;
  private transient long streamingWindowId;
  private transient TreeMap<Long, Long> streamingWindowToLatenessHorizon = new TreeMap<>();
  private ImplicitWatermarkGenerator implicitWatermarkGenerator;

  private Map<String, Component<Context.OperatorContext>> components = new HashMap<>();

  protected DataStorageT dataStorage;
  protected RetractionStorageT retractionStorage;
  protected AccumulationT accumulation;


  protected static final transient Collection<? extends Window> GLOBAL_WINDOW_SINGLETON_SET = Collections.singleton(Window.GlobalWindow.INSTANCE);

  private static final transient Logger LOG = LoggerFactory.getLogger(AbstractWindowedOperator.class);

  public final transient DefaultInputPort<Tuple<InputT>> input = new DefaultInputPort<Tuple<InputT>>()
  {
    @Override
    public void process(Tuple<InputT> tuple)
    {
      processTuple(tuple);
    }
  };

  // TODO: This port should be removed when Apex Core has native support for custom control tuples
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<ControlTuple> controlInput = new DefaultInputPort<ControlTuple>()
  {
    @Override
    public void process(ControlTuple tuple)
    {
      if (tuple instanceof ControlTuple.Watermark) {
        processWatermark((ControlTuple.Watermark)tuple);
      }
    }
  };

  // TODO: multiple input ports for join operations

  public final transient DefaultOutputPort<Tuple.WindowedTuple<OutputT>> output = new DefaultOutputPort<>();

  // TODO: This port should be removed when Apex Core has native support for custom control tuples
  public final transient DefaultOutputPort<ControlTuple> controlOutput = new DefaultOutputPort<>();

  /**
   * Process the incoming data tuple
   *
   * @param tuple the incoming tuple
   */
  public void processTuple(Tuple<InputT> tuple)
  {
    long timestamp = extractTimestamp(tuple, timestampExtractor);
    if (isTooLate(timestamp)) {
      dropTuple(tuple);
    } else {
      Tuple.WindowedTuple<InputT> windowedTuple = getWindowedValueWithTimestamp(tuple, timestamp);
      // do the accumulation
      accumulateTuple(windowedTuple);
      processWindowState(windowedTuple);
      if (implicitWatermarkGenerator != null) {
        implicitWatermarkGenerator.processTupleForWatermark(windowedTuple, currentDerivedTimestamp);
      }
    }
  }

  protected void processWindowState(Tuple.WindowedTuple<? extends Object> windowedTuple)
  {
    for (Window window : windowedTuple.getWindows()) {
      WindowState windowState = windowStateMap.get(window);
      windowState.tupleCount++;
      // process any count based triggers
      if (windowState.watermarkArrivalTime == -1) {
        // watermark has not arrived yet, check for early count based trigger
        if (earlyTriggerCount > 0 && (windowState.tupleCount % earlyTriggerCount) == 0) {
          fireTrigger(window, windowState);
        }
      } else {
        // watermark has arrived, check for late count based trigger
        if (lateTriggerCount > 0 && (windowState.tupleCount % lateTriggerCount) == 0) {
          fireTrigger(window, windowState);
        }
      }
    }
  }

  @Override
  public void setWindowOption(WindowOption windowOption)
  {
    this.windowOption = windowOption;
  }

  @Override
  public void setTriggerOption(TriggerOption triggerOption)
  {
    this.triggerOption = triggerOption;
    for (TriggerOption.Trigger trigger : triggerOption.getTriggerList()) {
      switch (trigger.getType()) {
        case ON_TIME:
          triggerAtWatermark = true;
          break;
        case EARLY:
          if (trigger instanceof TriggerOption.TimeTrigger) {
            earlyTriggerMillis = ((TriggerOption.TimeTrigger)trigger).getDuration().getMillis();
          } else if (trigger instanceof TriggerOption.CountTrigger) {
            earlyTriggerCount = ((TriggerOption.CountTrigger)trigger).getCount();
          }
          break;
        case LATE:
          if (trigger instanceof TriggerOption.TimeTrigger) {
            lateTriggerMillis = ((TriggerOption.TimeTrigger)trigger).getDuration().getMillis();
          } else if (trigger instanceof TriggerOption.CountTrigger) {
            lateTriggerCount = ((TriggerOption.CountTrigger)trigger).getCount();
          }
          break;
        default:
          throw new RuntimeException("Unknown trigger type: " + trigger.getType());
      }
    }
  }

  @Override
  public void setAllowedLateness(Duration allowedLateness)
  {
    this.allowedLatenessMillis = allowedLateness.getMillis();
  }

  /**
   * This method sets the storage for the data for each window
   *
   * @param dataStorage The data storage
   */
  public void setDataStorage(DataStorageT dataStorage)
  {
    this.dataStorage = dataStorage;
  }

  /**
   * This method sets the storage for the retraction data for each window. Only used when the accumulation mode is ACCUMULATING_AND_RETRACTING
   *
   * @param retractionStorage The retraction storage
   */
  public void setRetractionStorage(RetractionStorageT retractionStorage)
  {
    this.retractionStorage = retractionStorage;
  }

  public void addComponent(String key, Component<Context.OperatorContext> component)
  {
    components.put(key, component);
  }

  /**
   * Sets the accumulation, which basically tells the WindowedOperator what to do if a new tuple comes in and what
   * to put in the pane when a trigger is fired
   *
   * @param accumulation the accumulation
   */
  public void setAccumulation(AccumulationT accumulation)
  {
    this.accumulation = accumulation;
  }

  public void setWindowStateStorage(WindowedStorage.WindowedPlainStorage<WindowState> storageAgent)
  {
    this.windowStateMap = storageAgent;
  }

  @Override
  public void setTimestampExtractor(Function<InputT, Long> timestampExtractor)
  {
    this.timestampExtractor = timestampExtractor;
  }

  public void setNextWatermark(long timestamp)
  {
    this.nextWatermark = timestamp;
  }


  /**
   * Sets the fixed watermark with respect to the processing time derived from the Apex window ID. This is useful if we
   * don't have watermark tuples from upstream. However, using this means whether a tuple is considered late totally
   * depends on the Apex window ID of this operator.
   *
   * Note that setting this value will make incoming watermark tuples useless.
   */
  public void setFixedWatermark(long millis)
  {
    this.fixedWatermarkMillis = millis;
  }

  public void setImplicitWatermarkGenerator(ImplicitWatermarkGenerator implicitWatermarkGenerator)
  {
    this.implicitWatermarkGenerator = implicitWatermarkGenerator;
  }

  public void validate() throws ValidationException
  {
    if (accumulation == null) {
      throw new ValidationException("Accumulation must be set");
    }
    if (dataStorage == null) {
      throw new ValidationException("Data storage must be set");
    }
    if (windowStateMap == null) {
      throw new ValidationException("Window state storage must be set");
    }
    if (triggerOption != null) {
      if (triggerOption.isFiringOnlyUpdatedPanes()) {
        if (retractionStorage == null) {
          throw new ValidationException("A retraction storage is required for firingOnlyUpdatedPanes option");
        }
        if (triggerOption.getAccumulationMode() == TriggerOption.AccumulationMode.DISCARDING) {
          throw new ValidationException("DISCARDING accumulation mode is not valid for firingOnlyUpdatedPanes option");
        }
      }
      if (triggerOption.getAccumulationMode() == TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING &&
          retractionStorage == null) {
        throw new ValidationException("A retraction storage is required for ACCUMULATING_AND_RETRACTING accumulation mode");
      }
    }
  }

  @Override
  public Tuple.WindowedTuple<InputT> getWindowedValue(Tuple<InputT> input)
  {
    long timestamp = extractTimestamp(input, timestampExtractor);
    return getWindowedValueWithTimestamp(input, timestamp);
  }

  public <T> Tuple.WindowedTuple<T> getWindowedValueWithTimestamp(Tuple<T> input, long timestamp)
  {
    if (windowOption == null && input instanceof Tuple.WindowedTuple) {
      // inherit the windows from upstream
      initializeWindowStates(((Tuple.WindowedTuple<T>)input).getWindows());
      return (Tuple.WindowedTuple<T>)input;
    } else {
      return new Tuple.WindowedTuple<>(assignWindows(input, timestamp), timestamp, input.getValue());
    }
  }

  protected void initializeWindowStates(Collection<? extends Window> windows)
  {
    for (Window window : windows) {
      if (!windowStateMap.containsWindow(window)) {
        windowStateMap.put(window, new WindowState());
      }
    }
  }

  protected <T> long extractTimestamp(Tuple<T> tuple, Function<T, Long> timestampExtractor)
  {
    if (timestampExtractor == null) {
      if (tuple instanceof Tuple.TimestampedTuple) {
        return ((Tuple.TimestampedTuple)tuple).getTimestamp();
      } else {
        return 0;
      }
    } else {
      return timestampExtractor.apply(tuple.getValue());
    }
  }

  protected <T> Collection<? extends Window> assignWindows(Tuple<T> inputTuple, long timestamp)
  {
    if (windowOption instanceof WindowOption.GlobalWindow) {
      return GLOBAL_WINDOW_SINGLETON_SET;
    } else {
      if (windowOption instanceof WindowOption.TimeWindows) {
        Collection<? extends Window> windows = getTimeWindowsForTimestamp(timestamp);
        initializeWindowStates(windows);
        return windows;
      } else if (windowOption instanceof WindowOption.SessionWindows) {
        return assignSessionWindows(timestamp, inputTuple);
      } else {
        throw new IllegalStateException("Unsupported Window Option: " + windowOption.getClass());
      }
    }
  }

  protected <T> Collection<Window.SessionWindow> assignSessionWindows(long timestamp, Tuple<T> inputTuple)
  {
    throw new UnsupportedOperationException("Session window require keyed tuples");
  }

  /**
   * Returns the list of windows TimeWindows for the given timestamp.
   * If we are doing sliding windows, this will return multiple windows. Otherwise, only one window will be returned.
   * Note that this method does not apply to SessionWindows.
   *
   * @param timestamp the timestamp
   * @return the windows this timestamp belongs to
   */
  protected Collection<Window.TimeWindow> getTimeWindowsForTimestamp(long timestamp)
  {
    List<Window.TimeWindow> windows = new ArrayList<>();
    if (windowOption instanceof WindowOption.TimeWindows) {
      long durationMillis = ((WindowOption.TimeWindows)windowOption).getDuration().getMillis();
      long beginTimestamp = timestamp - timestamp % durationMillis;
      windows.add(new Window.TimeWindow(beginTimestamp, durationMillis));
      if (windowOption instanceof WindowOption.SlidingTimeWindows) {
        long slideBy = ((WindowOption.SlidingTimeWindows)windowOption).getSlideByDuration().getMillis();
        // add the sliding windows front and back
        for (long slideBeginTimestamp = beginTimestamp - slideBy;
            slideBeginTimestamp <= timestamp && timestamp < slideBeginTimestamp + durationMillis;
            slideBeginTimestamp -= slideBy) {
          windows.add(new Window.TimeWindow(slideBeginTimestamp, durationMillis));
        }
        for (long slideBeginTimestamp = beginTimestamp + slideBy;
            slideBeginTimestamp <= timestamp && timestamp < slideBeginTimestamp + durationMillis;
            slideBeginTimestamp += slideBy) {
          windows.add(new Window.TimeWindow(slideBeginTimestamp, durationMillis));
        }
      }
    } else {
      throw new IllegalStateException("Unexpected WindowOption");
    }
    return windows;
  }

  @Override
  public boolean isTooLate(long timestamp)
  {
    return allowedLatenessMillis >= 0 && (timestamp < currentWatermark - allowedLatenessMillis);
  }

  @Override
  public void dropTuple(Tuple input)
  {
    // do nothing
  }

  @Override
  public void processWatermark(ControlTuple.Watermark watermark)
  {
    this.nextWatermark = watermark.getTimestamp();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setup(Context.OperatorContext context)
  {
    this.timeIncrement = context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    validate();
    windowStateMap.setup(context);
    dataStorage.setup(context);
    if (retractionStorage != null) {
      retractionStorage.setup(context);
    }
    if (implicitWatermarkGenerator != null) {
      implicitWatermarkGenerator.setup(context);
    }
    for (Component component : components.values()) {
      component.setup(context);
    }
    if (this.windowOption instanceof WindowOption.GlobalWindow) {
      windowStateMap.put(Window.GlobalWindow.INSTANCE, new WindowState());
    }
  }

  @Override
  public void teardown()
  {
    windowStateMap.teardown();
    dataStorage.teardown();
    if (retractionStorage != null) {
      retractionStorage.teardown();
    }
    if (implicitWatermarkGenerator != null) {
      implicitWatermarkGenerator.teardown();
    }
    for (Component component : components.values()) {
      component.teardown();
    }
  }

  /**
   * This is for the Apex streaming/application window. Do not confuse this with the windowing concept in this operator
   */
  @Override
  public void beginWindow(long windowId)
  {
    for (Component component : components.values()) {
      if (component instanceof WindowListener) {
        ((WindowListener)component).beginWindow(windowId);
      }
    }
    if (currentDerivedTimestamp == -1) {
      // TODO: once we are able to get the firstWindowMillis from Apex Core API, we should use that instead
      currentDerivedTimestamp = (windowId >> 32) * 1000;
    } else {
      currentDerivedTimestamp += timeIncrement;
    }
    streamingWindowId = windowId;
  }

  /**
   * This is for the Apex streaming/application window. Do not confuse this with the windowing concept in this operator
   */
  @Override
  public void endWindow()
  {
    // We only do actual processing of watermark at window boundary so that it will not break idempotency.
    // TODO: May want to revisit this if the application cares more about latency than idempotency
    processWatermarkAtEndWindow();
    fireTimeTriggers();

    for (Component component : components.values()) {
      if (component instanceof WindowListener) {
        ((WindowListener)component).endWindow();
      }
    }
  }

  protected void processWatermarkAtEndWindow()
  {
    long implicitWatermark = -1;
    if (implicitWatermarkGenerator != null) {
      implicitWatermark = implicitWatermarkGenerator
          .getWatermarkTuple(currentDerivedTimestamp).getTimestamp();
    }
    if (implicitWatermark > nextWatermark) {
      nextWatermark = implicitWatermark;
    }

    if (nextWatermark > 0 && currentWatermark < nextWatermark) {

      long horizon = nextWatermark - allowedLatenessMillis;

      for (Iterator<Map.Entry<Window, WindowState>> it = windowStateMap.entries().iterator(); it.hasNext(); ) {
        Map.Entry<Window, WindowState> entry = it.next();
        Window window = entry.getKey();
        WindowState windowState = entry.getValue();
        if (window.getBeginTimestamp() + window.getDurationMillis() < nextWatermark) {
          // watermark has not arrived for this window before, marking this window late
          if (windowState.watermarkArrivalTime == -1) {
            windowState.watermarkArrivalTime = currentDerivedTimestamp;
            if (triggerAtWatermark) {
              // fire trigger at watermark if applicable
              fireTrigger(window, windowState);
            }
          }
          if (allowedLatenessMillis >= 0 && window.getBeginTimestamp() + window.getDurationMillis() < horizon) {
            // discard this window because it's too late now
            it.remove();
            dataStorage.remove(window);
            if (retractionStorage != null) {
              retractionStorage.remove(window);
            }
          }
        }
      }
      streamingWindowToLatenessHorizon.put(streamingWindowId, horizon);
      controlOutput.emit(new WatermarkImpl(nextWatermark));
      this.currentWatermark = nextWatermark;
    }
  }

  private void fireTimeTriggers()
  {
    if (earlyTriggerMillis > 0 || lateTriggerMillis > 0) {
      for (Map.Entry<Window, WindowState> entry : windowStateMap.entries()) {
        Window window = entry.getKey();
        WindowState windowState = entry.getValue();
        if (windowState.watermarkArrivalTime == -1) {
          if (earlyTriggerMillis > 0 && windowState.lastTriggerFiredTime + earlyTriggerMillis <= currentDerivedTimestamp) {
            // fire early time triggers
            fireTrigger(window, windowState);
          }
        } else {
          if (lateTriggerMillis > 0 && windowState.lastTriggerFiredTime + lateTriggerMillis <= currentDerivedTimestamp) {
            // fire late time triggers
            fireTrigger(window, windowState);
          }
        }
      }
    }
  }

  protected boolean isFiringOnlyUpdatedPanes()
  {
    return triggerOption.isFiringOnlyUpdatedPanes();
  }

  @Override
  public void fireTrigger(Window window, WindowState windowState)
  {
    if (triggerOption.getAccumulationMode() == TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      fireRetractionTrigger(window, isFiringOnlyUpdatedPanes());
    }
    fireNormalTrigger(window, triggerOption.isFiringOnlyUpdatedPanes());
    windowState.lastTriggerFiredTime = currentDerivedTimestamp;
    if (triggerOption.getAccumulationMode() == TriggerOption.AccumulationMode.DISCARDING) {
      clearWindowData(window);
    }
  }

  DataStorageT getDataStorage()
  {
    return dataStorage;
  }

  AccumulationT getAccumulation()
  {
    return accumulation;
  }

  /**
   * This method fires the normal trigger for the given window.
   *
   * @param window the window to fire trigger on
   * @param fireOnlyUpdatedPanes Do not fire trigger if the old value is the same as the new value. If true, retraction storage is required.
   */
  public abstract void fireNormalTrigger(Window window, boolean fireOnlyUpdatedPanes);

  /**
   * This method fires the retraction trigger for the given window. This should only be valid if the accumulation
   * mode is ACCUMULATING_AND_RETRACTING
   *
   * @param window the window to fire the retraction trigger on
   * @param fireOnlyUpdatedPanes Do not fire trigger if the retraction value is the same as the new value.
   */
  public abstract void fireRetractionTrigger(Window window, boolean fireOnlyUpdatedPanes);


  @Override
  public void clearWindowData(Window window)
  {
    dataStorage.remove(window);
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    for (Component component : components.values()) {
      if (component instanceof CheckpointNotificationListener) {
        ((CheckpointNotificationListener)component).beforeCheckpoint(windowId);
      }
    }
  }

  @Override
  public void checkpointed(long windowId)
  {
    for (Component component : components.values()) {
      if (component instanceof CheckpointNotificationListener) {
        ((CheckpointNotificationListener)component).checkpointed(windowId);
      }
    }
  }

  @Override
  public void committed(long windowId)
  {
    for (Component component : components.values()) {
      if (component instanceof CheckpointNotificationListener) {
        ((CheckpointNotificationListener)component).committed(windowId);
      }
    }
    Long floorWindowId = streamingWindowToLatenessHorizon.floorKey(windowId);
    if (floorWindowId != null) {
      long horizon = streamingWindowToLatenessHorizon.get(floorWindowId);
      windowStateMap.purge(horizon);
      dataStorage.purge(horizon);
      if (retractionStorage != null) {
        retractionStorage.purge(horizon);
      }
      streamingWindowToLatenessHorizon.headMap(windowId, true).clear();
    }
  }
}
