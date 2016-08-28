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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.SessionWindowedStorage;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.WindowedKeyedStorage;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.lib.util.KeyValPair;

/**
 * This is an implementation of WindowedOperator that takes in key value pairs as input and gives out key value pairs
 * as output. If your operation is not key based, please use {@link WindowedOperatorImpl}.
 *
 * @param <KeyT> The type of the key of both the input and the output tuple
 * @param <InputValT> The type of the value of the keyed input tuple
 * @param <AccumT> The type of the accumulated value in the operator state per key per window
 * @param <OutputValT> The type of the value of the keyed output tuple
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class KeyedWindowedOperatorImpl<KeyT, InputValT, AccumT, OutputValT>
    extends AbstractWindowedOperator<KeyValPair<KeyT, InputValT>, KeyValPair<KeyT, OutputValT>, WindowedKeyedStorage<KeyT, AccumT>, WindowedKeyedStorage<KeyT, OutputValT>, Accumulation<InputValT, AccumT, OutputValT>>
{

  @Override
  protected void assignSessionWindows(List<Window> windows, long timestamp, Tuple<KeyValPair<KeyT, InputValT>> inputTuple)
  {
    KeyT key = inputTuple.getValue().getKey();
    WindowOption.SessionWindows sessionWindowOption = (WindowOption.SessionWindows)windowOption;
    SessionWindowedStorage<KeyT, AccumT> sessionStorage = (SessionWindowedStorage<KeyT, AccumT>)dataStorage;
    Collection<Map.Entry<Window.SessionWindow<KeyT>, AccumT>> sessionEntries = sessionStorage.getSessionEntries(key, timestamp, sessionWindowOption.getMinGap().getMillis());
    Window.SessionWindow<KeyT> sessionWindowToAssign;
    switch (sessionEntries.size()) {
      case 0: {
        // There are no existing windows within the minimum gap. Create a new session window
        Window.SessionWindow<KeyT> sessionWindow = new Window.SessionWindow<>(key, timestamp, 1);
        windowStateMap.put(sessionWindow, new WindowState());
        sessionWindowToAssign = sessionWindow;
        break;
      }
      case 1: {
        // There is already one existing window within the minimum gap. See whether we need to extend the time of that window
        Map.Entry<Window.SessionWindow<KeyT>, AccumT> sessionWindowEntry = sessionEntries.iterator().next();
        Window.SessionWindow<KeyT> sessionWindow = sessionWindowEntry.getKey();
        if (sessionWindow.getBeginTimestamp() <= timestamp && timestamp < sessionWindow.getBeginTimestamp() + sessionWindow.getDurationMillis()) {
          // The session window already covers the event
          sessionWindowToAssign = sessionWindow;
        } else {
          // The session window does not cover the event but is within the min gap
          if (triggerOption != null &&
              triggerOption.getAccumulationMode() == TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
            // fire a retraction trigger because the session window will be enlarged
            fireRetractionTrigger(sessionWindow);
          }
          // create a new session window that covers the timestamp
          long newBeginTimestamp = Math.min(sessionWindow.getBeginTimestamp(), timestamp);
          long newEndTimestamp = Math.max(sessionWindow.getBeginTimestamp() + sessionWindow.getDurationMillis(), timestamp + 1);
          Window.SessionWindow<KeyT> newSessionWindow =
              new Window.SessionWindow<>(key, newBeginTimestamp, newEndTimestamp - newBeginTimestamp);
          windowStateMap.remove(sessionWindow);
          sessionStorage.migrateWindow(sessionWindow, newSessionWindow);
          windowStateMap.put(newSessionWindow, new WindowState());
          sessionWindowToAssign = newSessionWindow;
        }
        break;
      }
      case 2: {
        // There are two windows that fall within the minimum gap of the timestamp. We need to merge the two windows
        Iterator<Map.Entry<Window.SessionWindow<KeyT>, AccumT>> iterator = sessionEntries.iterator();
        Map.Entry<Window.SessionWindow<KeyT>, AccumT> sessionWindowEntry1 = iterator.next();
        Map.Entry<Window.SessionWindow<KeyT>, AccumT> sessionWindowEntry2 = iterator.next();
        Window.SessionWindow<KeyT> sessionWindow1 = sessionWindowEntry1.getKey();
        Window.SessionWindow<KeyT> sessionWindow2 = sessionWindowEntry2.getKey();
        AccumT sessionData1 = sessionWindowEntry1.getValue();
        AccumT sessionData2 = sessionWindowEntry2.getValue();
        if (triggerOption != null &&
            triggerOption.getAccumulationMode() == TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
          // fire a retraction trigger because the two session windows will be merged to a new window
          fireRetractionTrigger(sessionWindow1);
          fireRetractionTrigger(sessionWindow2);
        }
        long newBeginTimestamp = Math.min(sessionWindow1.getBeginTimestamp(), sessionWindow2.getBeginTimestamp());
        long newEndTimestamp = Math.max(sessionWindow1.getBeginTimestamp() + sessionWindow1.getDurationMillis(),
            sessionWindow2.getBeginTimestamp() + sessionWindow2.getDurationMillis());

        Window.SessionWindow<KeyT> newSessionWindow = new Window.SessionWindow<>(key, newBeginTimestamp, newEndTimestamp - newBeginTimestamp);
        AccumT newSessionData = accumulation.merge(sessionData1, sessionData2);
        sessionStorage.remove(sessionWindow1);
        sessionStorage.remove(sessionWindow2);
        sessionStorage.put(newSessionWindow, key, newSessionData);
        windowStateMap.remove(sessionWindow1);
        windowStateMap.remove(sessionWindow2);
        windowStateMap.put(newSessionWindow, new WindowState());
        sessionWindowToAssign = newSessionWindow;
        break;
      }
      default:
        throw new IllegalStateException("There are more than two sessions matching one timestamp");
    }
    windows.add(sessionWindowToAssign);
  }

  @Override
  public void accumulateTuple(Tuple.WindowedTuple<KeyValPair<KeyT, InputValT>> tuple)
  {
    KeyValPair<KeyT, InputValT> kvData = tuple.getValue();
    KeyT key = kvData.getKey();
    for (Window window : tuple.getWindows()) {
      // process each window
      AccumT accum = dataStorage.get(window, key);
      if (accum == null) {
        accum = accumulation.defaultAccumulatedValue();
      }
      dataStorage.put(window, key, accumulation.accumulate(accum, kvData.getValue()));
    }
  }

  @Override
  public void fireNormalTrigger(Window window, boolean fireOnlyUpdatedPanes)
  {
    for (Map.Entry<KeyT, AccumT> entry : dataStorage.entrySet(window)) {
      OutputValT outputVal = accumulation.getOutput(entry.getValue());
      if (fireOnlyUpdatedPanes) {
        OutputValT oldValue = retractionStorage.get(window, entry.getKey());
        if (oldValue != null && oldValue.equals(outputVal)) {
          continue;
        }
      }
      output.emit(new Tuple.WindowedTuple<>(window, new KeyValPair<>(entry.getKey(), outputVal)));
      if (retractionStorage != null) {
        retractionStorage.put(window, entry.getKey(), outputVal);
      }
    }
  }

  @Override
  public void fireRetractionTrigger(Window window)
  {
    if (triggerOption.getAccumulationMode() != TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      throw new UnsupportedOperationException();
    }
    for (Map.Entry<KeyT, OutputValT> entry : retractionStorage.entrySet(window)) {
      output.emit(new Tuple.WindowedTuple<>(window, new KeyValPair<>(entry.getKey(), accumulation.getRetraction(entry.getValue()))));
    }
  }

}
