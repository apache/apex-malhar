/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import java.util.Iterator;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Lists;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * <p>
 * This operator searches for a given pattern in the input stream.<br>
 * This takes a pattern which is a list of events that you want to find.<br>
 * For e.g. If the pattern is defined as “aa” and your input events arrive in following manner “a”, “a”, “a”, then this operator
 * will emit 2 matches for the given pattern. One matching tuple 1 and 2 and other matching 2 and 3.
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
 */

@OperatorAnnotation(partitionable = false)
public abstract class AbstractStreamPatternMatcher<T> extends BaseOperator
{
  @NotNull
  private Pattern<T> pattern;

  // this stores the partial matches found so far
  private List<MutableInt> partialMatches = Lists.newLinkedList();
  private transient MutableInt patternLength;

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

  public Pattern<T> getPattern()
  {
    return pattern;
  }

  public transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      if (partialMatches.size() > 0) {
        MutableInt tempInt;
        Iterator<MutableInt> itr = partialMatches.iterator();
        while (itr.hasNext()) {
          tempInt = itr.next();
          tempInt.increment();
          if (!pattern.checkState(t, tempInt.intValue())) {
            itr.remove();
          }
        }
      }
      if (pattern.checkState(t, 0)) {
        partialMatches.add(new MutableInt(0));
      }
      if (partialMatches.remove(patternLength)) {
        processPatternFound();
      }
    }
  };

  public abstract void processPatternFound();

  public static class Pattern<T>
  {
    @NotNull
    private final T[] states;

    public Pattern(@NotNull T[] states)
    {
      this.states = states;
    }

    public boolean checkState(T t, int position)
    {
      return states[position].equals(t);
    }

    public T[] getStates()
    {
      return states;
    }

  }
}