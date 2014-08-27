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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This operator searches the pattern in the input stream.
 *
 * <br>
 * <b> StateFull : Yes, </b> Count is aggregated over application window(s). <br>
 * <b> Partitionable : No, </b> will yield wrong result. <br>
 *
 * <b>Ports</b>:<br>
 * <b>inputPort</b>: the port to receive input<br>
 *
 * <b>Properties</b>:<br>
 * <b>pattern</b>: The pattern that needs to be searched<br>
 */

@OperatorAnnotation(partitionable = false)
public abstract class AbstractPatternMatcher<T> extends BaseOperator
{
  @NotNull
  private Pattern pattern;
  private List<List<T>> matchedPatterns;
  private int patternLength;

  public AbstractPatternMatcher()
  {
    matchedPatterns = Lists.newArrayList();
  }

  public void setPattern(Pattern pattern)
  {
    this.pattern = pattern;
    patternLength = pattern.getPattern().size();
    matchedPatterns.clear();
    for (int i = 0; i < patternLength; i++) {
      matchedPatterns.add(new ArrayList<T>());
    }
  }

  public Pattern getPattern()
  {
    return pattern;
  }

  public transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      List<Integer> matchingPositions = pattern.getPosition(t);
      if (matchingPositions == null) {
        for (int i = 0; i < patternLength; i++) {
          matchedPatterns.get(i).clear();
        }
        return;
      }
      int matchingPositionIndex = 0;
      int patternPosition = matchingPositions.get(matchingPositionIndex);
      int matchingPositionLength = matchingPositions.size();
      for (int i = 0; i < patternLength; ) {
        while (i < patternLength - patternPosition) {
          matchedPatterns.get(i).clear();
          i++;
        }
        if (i == patternLength) {
          break;
        }
        List<T> prev = matchedPatterns.get(i);
        if (!prev.isEmpty()) {
          prev.add(t);
          matchedPatterns.get(i - 1).addAll(prev);
          prev.clear();
        }
        matchingPositionIndex++;
        i++;
        if (matchingPositionIndex == matchingPositionLength) {
          for (; i < patternLength; i++) {
            matchedPatterns.get(i).clear();
          }
          break;
        }
        else {
          patternPosition = matchingPositions.get(matchingPositionIndex);
        }
      }
      if (patternPosition == 0) {
        matchedPatterns.get(patternLength - 1).add(t);
      }
      List<T> outputList = matchedPatterns.get(0);
      if (!outputList.isEmpty()) {
        processPatternFound(outputList);
        outputList.clear();
      }
    }
  };

  public abstract void processPatternFound(List<T> outputList);

  public transient DefaultOutputPort<List<T>> outputPort = new DefaultOutputPort<List<T>>();

  public static class Pattern<T>
  {
    private Map<T, List<Integer>> positionMap;
    private List<T> pattern;

    public Pattern()
    {
      positionMap = Maps.newHashMap();
    }

    public Pattern(List<T> pattern)
    {
      this.pattern = pattern;
      positionMap = Maps.newHashMap();
      populatePositionMap();
    }

    private void populatePositionMap()
    {
      for (int i = 0; i < pattern.size(); i++) {
        if (positionMap.get(pattern.get(i)) == null) {
          positionMap.put(pattern.get(i), new ArrayList<Integer>());
        }
        positionMap.get(pattern.get(i)).add(0, i);
      }
    }

    public List<Integer> getPosition(T t)
    {
      return positionMap.get(t);
    }

    public List<T> getPattern()
    {
      return pattern;
    }

    public void setPattern(List<T> pattern)
    {
      this.pattern = pattern;
      positionMap.clear();
      populatePositionMap();
    }
  }
}
