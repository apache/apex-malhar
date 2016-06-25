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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.validation.ValidationException;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.WindowedOperator;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Function;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
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
 * @param <AccumulationT> The type of the accumulation
 */
@InterfaceStability.Evolving
public abstract class AbstractWindowedOperator<InputT, OutputT, DataStorageT extends WindowedStorage, AccumulationT extends Accumulation>
    extends BaseOperator implements WindowedOperator<InputT>
{

  protected WindowOption windowOption;
  protected TriggerOption triggerOption;
  protected long allowedLatenessMillis = -1;
  protected WindowedStorage<WindowState> windowStateMap;

  private Function<InputT, Long> timestampExtractor;

  private long currentWatermark;
  private boolean triggerAtWatermark;
  private long earlyTriggerCount;
  private long earlyTriggerMillis;
  private long lateTriggerCount;
  private long lateTriggerMillis;
  private long currentDerivedTimestamp = -1;
  private long windowWidthMillis;
  protected DataStorageT dataStorage;
  protected DataStorageT retractionStorage;
  protected AccumulationT accumulation;

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

  public final transient DefaultOutputPort<Tuple<OutputT>> output = new DefaultOutputPort<>();

  // TODO: This port should be removed when Apex Core has native support for custom control tuples
  public final transient DefaultOutputPort<ControlTuple> controlOutput = new DefaultOutputPort<>();

  /**
   * Process the incoming data tuple
   *
   * @param tuple
   */
  public void processTuple(Tuple<InputT> tuple)
  {
    long timestamp = extractTimestamp(tuple);
    if (isTooLate(timestamp)) {
      dropTuple(tuple);
    } else {
      Tuple.WindowedTuple<InputT> windowedTuple = getWindowedValue(tuple);
      // do the accumulation
      accumulateTuple(windowedTuple);

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
  }

  @Override
  public void setWindowOption(WindowOption windowOption)
  {
    this.windowOption = windowOption;
    if (this.windowOption instanceof WindowOption.GlobalWindow) {
      windowStateMap.put(Window.GLOBAL_WINDOW, new WindowState());
    }
  }

  @Override
  public void setTriggerOption(TriggerOption triggerOption)
  {
    this.triggerOption = triggerOption;
    for (TriggerOption.Trigger trigger : triggerOption.getTriggerList()) {
      switch (trigger.getWatermarkOpt()) {
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
          throw new RuntimeException("Unexpected watermark option: " + trigger.getWatermarkOpt());
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
   * @param storageAgent
   */
  public void setDataStorage(DataStorageT storageAgent)
  {
    this.dataStorage = storageAgent;
  }

  /**
   * This method sets the storage for the retraction data for each window. Only used when the accumulation mode is ACCUMULATING_AND_RETRACTING
   *
   * @param storageAgent
   */
  public void setRetractionStorage(DataStorageT storageAgent)
  {
    this.retractionStorage = storageAgent;
  }

  /**
   * Sets the accumulation, which basically tells the WindowedOperator what to do if a new tuple comes in and what
   * to put in the pane when a trigger is fired
   *
   * @param accumulation
   */
  public void setAccumulation(AccumulationT accumulation)
  {
    this.accumulation = accumulation;
  }

  @Override
  public void setWindowStateStorage(WindowedStorage<WindowState> storageAgent)
  {
    this.windowStateMap = storageAgent;
  }

  @Override
  public void setTimestampExtractor(Function<InputT, Long> timestampExtractor)
  {
    this.timestampExtractor = timestampExtractor;
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
    Tuple.WindowedTuple<InputT> windowedTuple = new Tuple.WindowedTuple<>();
    windowedTuple.setValue(input.getValue());
    windowedTuple.setTimestamp(extractTimestamp(input));
    assignWindows(windowedTuple.getWindows(), input);
    return windowedTuple;
  }

  private long extractTimestamp(Tuple<InputT> tuple)
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

  private void assignWindows(List<Window> windows, Tuple<InputT> inputTuple)
  {
    if (windowOption instanceof WindowOption.GlobalWindow) {
      windows.add(Window.GLOBAL_WINDOW);
    } else {
      long timestamp = extractTimestamp(inputTuple);
      if (windowOption instanceof WindowOption.TimeWindows) {

        for (Window.TimeWindow window : getTimeWindowsForTimestamp(timestamp)) {
          if (!windowStateMap.containsWindow(window)) {
            windowStateMap.put(window, new WindowState());
          }
          windows.add(window);
        }
      } else if (windowOption instanceof WindowOption.SessionWindows) {
        assignSessionWindows(windows, timestamp, inputTuple);
      }
    }
  }

  protected void assignSessionWindows(List<Window> windows, long timestamp, Tuple<InputT> inputTuple)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the list of windows TimeWindows for the given timestamp.
   * If we are doing sliding windows, this will return multiple windows. Otherwise, only one window will be returned.
   * Note that this method does not apply to SessionWindows.
   *
   * @param timestamp
   * @return
   */
  private List<Window.TimeWindow> getTimeWindowsForTimestamp(long timestamp)
  {
    List<Window.TimeWindow> windows = new ArrayList<>();
    if (windowOption instanceof WindowOption.TimeWindows) {
      long durationMillis = ((WindowOption.TimeWindows)windowOption).getDuration().getMillis();
      long beginTimestamp = timestamp - timestamp % durationMillis;
      windows.add(new Window.TimeWindow(beginTimestamp, durationMillis));
      if (windowOption instanceof WindowOption.SlidingTimeWindows) {
        long slideBy = ((WindowOption.SlidingTimeWindows)windowOption).getSlideByDuration().getMillis();
        // add the sliding windows front and back
        // Note: this messes up the order of the window and we might want to revisit this if the order of the windows
        // matter
        for (long slideBeginTimestamp = beginTimestamp - slideBy;
            slideBeginTimestamp >= timestamp && timestamp > slideBeginTimestamp + durationMillis;
            slideBeginTimestamp -= slideBy) {
          windows.add(new Window.TimeWindow(slideBeginTimestamp, durationMillis));
        }
        for (long slideBeginTimestamp = beginTimestamp + slideBy;
            slideBeginTimestamp >= timestamp && timestamp > slideBeginTimestamp + durationMillis;
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
    return allowedLatenessMillis < 0 ? false : (timestamp < currentWatermark - allowedLatenessMillis);
  }

  @Override
  public void dropTuple(Tuple<InputT> input)
  {
    // do nothing
    LOG.debug("Dropping late tuple {}", input);
  }


  @Override
  public void processWatermark(ControlTuple.Watermark watermark)
  {
    currentWatermark = watermark.getTimestamp();
    long horizon = currentWatermark - allowedLatenessMillis;
    if (allowedLatenessMillis >= 0) {
      // purge window that are too late to accept any more input
      dataStorage.removeUpTo(horizon);
    }

    for (Iterator<Map.Entry<Window, WindowState>> it = windowStateMap.iterator(); it.hasNext(); ) {
      Map.Entry<Window, WindowState> entry = it.next();
      Window window = entry.getKey();
      WindowState windowState = entry.getValue();
      if (allowedLatenessMillis >= 0 && window.getBeginTimestamp() + window.getDurationMillis() < horizon) {
        // discard this window because it's too late now
        it.remove();
      } else if (window.getBeginTimestamp() + window.getDurationMillis() < currentWatermark) {
        // watermark has not arrived for this window before, marking this window late
        if (windowState.watermarkArrivalTime == -1) {
          windowState.watermarkArrivalTime = currentDerivedTimestamp;
          if (triggerAtWatermark) {
            // fire trigger at watermark if applicable
            fireTrigger(window, windowState);
          }
        }
      }
    }
    controlOutput.emit(watermark);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.windowWidthMillis = context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    validate();
  }

  /**
   * This is for the Apex streaming/application window. Do not confuse this with the windowing concept in this operator
   */
  @Override
  public void beginWindow(long windowId)
  {
    if (currentDerivedTimestamp == -1) {
      currentDerivedTimestamp = ((windowId >> 32) * 1000) + (windowId & 0xffffffffL);
    } else {
      currentDerivedTimestamp += windowWidthMillis;
    }
  }

  /**
   * This is for the Apex streaming/application window. Do not confuse this with the windowing concept in this operator
   */
  @Override
  public void endWindow()
  {
    fireTimeTriggers();
  }

  private void fireTimeTriggers()
  {
    if (earlyTriggerMillis > 0 || lateTriggerMillis > 0) {
      for (Map.Entry<Window, WindowState> entry : windowStateMap.entrySet()) {
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

  @Override
  public void fireTrigger(Window window, WindowState windowState)
  {
    if (triggerOption.getAccumulationMode() == TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      fireRetractionTrigger(window);
    }
    fireNormalTrigger(window, triggerOption.isFiringOnlyUpdatedPanes());
    windowState.lastTriggerFiredTime = currentDerivedTimestamp;
    if (triggerOption.getAccumulationMode() == TriggerOption.AccumulationMode.DISCARDING) {
      clearWindowData(window);
    }
  }

  /**
   * This method fires the normal trigger for the given window.
   *
   * @param window
   * @param fireOnlyUpdatedPanes Do not fire trigger if the old value is the same as the new value. If true, retraction storage is required.
   */
  public abstract void fireNormalTrigger(Window window, boolean fireOnlyUpdatedPanes);

  /**
   * This method fires the retraction trigger for the given window. This should only be valid if the accumulation
   * mode is ACCUMULATING_AND_RETRACTING
   *
   * @param window
   */
  public abstract void fireRetractionTrigger(Window window);


  @Override
  public void clearWindowData(Window window)
  {
    dataStorage.remove(window);
  }

  @Override
  public void invalidateWindow(Window window)
  {
    dataStorage.remove(window);
    if (retractionStorage != null) {
      retractionStorage.remove(window);
    }
    windowStateMap.remove(window);
  }
}
