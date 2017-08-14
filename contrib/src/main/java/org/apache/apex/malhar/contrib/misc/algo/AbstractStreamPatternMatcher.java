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
package org.apache.apex.malhar.contrib.misc.algo;

import java.util.Iterator;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * <p>
 * This operator searches for a given pattern in the input stream.<br>
 * For e.g. If the pattern is defined as “aa” and your input events arrive in following manner “a”, “a”, “a”, then this operator
 * will emit 2 matches for the given pattern. One matching event 1 and 2 and other matching 2 and 3.
 * </p>
 *
 * <br>
 * <b> StateFull : Yes, </b> Pattern is found over application window(s). <br>
 * <b> Partitionable : No, </b> will yield wrong result. <br>
 *
 * <br>
 * <b>Ports</b>:<br>
 * <b>inputPort</b>: the port to receive input<br>
 *
 * <br>
 * <b>Properties</b>:<br>
 * <b>pattern</b>: The pattern that needs to be searched<br>
 *
 * @param <T> event type
 *
 * @since 2.0.0
 * @deprecated
 */
@Deprecated
@OperatorAnnotation(partitionable = false)
public abstract class AbstractStreamPatternMatcher<T> extends BaseOperator
{
  /**
   * The pattern to be searched in the input stream of events
   */
  @NotNull
  private Pattern<T> pattern;

  // this stores the index of the partial matches found so far
  private List<MutableInt> partialMatches = Lists.newLinkedList();
  private transient MutableInt patternLength;

  /**
   * Set the pattern that needs to be searched in the input stream of events
   *
   * @param pattern The pattern to be searched
   */
  public void setPattern(Pattern<T> pattern)
  {
    this.pattern = pattern;
    partialMatches.clear();
    patternLength = new MutableInt(pattern.getStates().length - 1);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    patternLength = new MutableInt(pattern.getStates().length - 1);
  }

  /**
   * Get the pattern that is searched in the input stream of events
   *
   * @return Returns the pattern searched
   */
  public Pattern<T> getPattern()
  {
    return pattern;
  }

  public transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      if (pattern.checkState(t, 0)) {
        partialMatches.add(new MutableInt(-1));
      }
      if (partialMatches.size() > 0) {
        MutableInt tempInt;
        Iterator<MutableInt> itr = partialMatches.iterator();
        while (itr.hasNext()) {
          tempInt = itr.next();
          tempInt.increment();
          if (!pattern.checkState(t, tempInt.intValue())) {
            itr.remove();
          } else if (tempInt.equals(patternLength)) {
            itr.remove();
            processPatternFound();
          }
        }
      }
    }
  };

  /**
   * This function determines how to process the pattern found
   */
  public abstract void processPatternFound();

  public static class Pattern<T>
  {
    /**
     * The states of the pattern
     */
    @NotNull
    private final T[] states;

    //for kryo
    private Pattern()
    {
      states = null;
    }

    public Pattern(@NotNull T[] states)
    {
      this.states = states;
    }

    /**
     * Checks if the input state matches the state at index "index" of the pattern
     *
     * @param t     The input state
     * @param index The index to match in the pattern
     * @return True if the state exists at index "index" else false
     */
    public boolean checkState(T t, int index)
    {
      return states[index].equals(t);
    }

    /**
     * Get the states of the pattern
     *
     * @return The states of the pattern
     */
    public T[] getStates()
    {
      return states;
    }

  }
}
