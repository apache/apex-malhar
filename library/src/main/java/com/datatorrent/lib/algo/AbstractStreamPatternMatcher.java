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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
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
  private ArrayList<List<T>> matchedPatterns;
  private int patternLength;

  public AbstractStreamPatternMatcher()
  {
    matchedPatterns = Lists.newArrayList();
  }

  public void setPattern(Pattern<T> pattern)
  {
    this.pattern = pattern;
    patternLength = pattern.getStates().length;
    matchedPatterns.clear();
    for (int i = 0; i < patternLength; i++) {
      matchedPatterns.add(null);
    }
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
      if (pattern == null) {
        logger.error("Please set the pattern before searching");
        return;
      }
      //get the matches for input event in the pattern
      List<Integer> matchingPositions = pattern.getPosition(t);
      List<T> currentMatch;

      /**
       * Algorithm :
       * Store all the partial matches seen so far.
       * Find the position(s) of the input event in the given pattern.
       * If there was no matching position then reset all the existing partial matches
       * For each of the matching position x, if there was a partial match till position x-1, then append the new event to this partial match.
       * Reset partial matches for all non matching positions
       */

      // if there is no match then reset all the existing partial matches to null
      if (matchingPositions == null) {
        for (int i = 0; i < patternLength; i++) {
          matchedPatterns.set(i, null);
        }
        return;
      }
      int matchingPositionIndex = 0;
      int patternPosition = matchingPositions.get(matchingPositionIndex);
      int matchingPositionLength = matchingPositions.size();

      for (int i = 0; i < patternLength; ) {
        //Reset partial matches to null for all non matching positions
        while (i < patternLength - patternPosition) {
          matchedPatterns.set(i, null);
          i++;
        }
        if (i == patternLength) {
          break;
        }
        currentMatch = matchedPatterns.get(i);
        //If there was a partial match till position x-1, then append the new event to this partial match.
        if (currentMatch != null) {
          currentMatch.add(t);
          matchedPatterns.set(i - 1, currentMatch);
          matchedPatterns.set(i, null);
        }
        matchingPositionIndex++;
        i++;
        //Reset partial matches to null for all non matching positions
        if (matchingPositionIndex == matchingPositionLength) {
          for (; i < patternLength; i++) {
            matchedPatterns.set(i, null);
          }
          break;
        }
        else {
          patternPosition = matchingPositions.get(matchingPositionIndex);
        }
      }
      // If the match is found at starting of pattern state,then initialize the partial match
      if (patternPosition == 0) {
        currentMatch = Lists.newArrayList();
        currentMatch.add(t);
        matchedPatterns.set(patternLength - 1, currentMatch);
      }
      List<T> outputList = matchedPatterns.get(0);
      // If the match is found process it
      if (outputList != null) {
        processPatternFound(outputList);
        matchedPatterns.set(0, null);
      }
    }
  };

  public abstract void processPatternFound(List<T> outputList);

  public transient DefaultOutputPort<List<T>> outputPort = new DefaultOutputPort<List<T>>();

  public static class Pattern<T>
  {
    private Map<T, List<Integer>> positionMap;
    private T[] states;

    public Pattern()
    {
      positionMap = Maps.newHashMap();
    }

    public Pattern(T[] states)
    {
      this.states = states;
      positionMap = Maps.newHashMap();
      populatePositionMap();
    }

    /**
     * This function pre-calculates the position of each of the state in the pattern
     */
    private void populatePositionMap()
    {
      for (int i = 0; i < states.length; i++) {
        if (positionMap.get(states[i]) == null) {
          positionMap.put(states[i], new ArrayList<Integer>());
        }
        positionMap.get(states[i]).add(0, i);
      }
    }

    public List<Integer> getPosition(T t)
    {
      return positionMap.get(t);
    }

    public T[] getStates()
    {
      return states;
    }

    public void setStates(T[] states)
    {
      this.states = states;
      positionMap.clear();
      populatePositionMap();
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractStreamPatternMatcher.class);
}