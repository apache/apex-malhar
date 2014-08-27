package com.datatorrent.lib.algo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 *
 */

public class PatternMatcher<T> extends BaseOperator
{

  private List<List<T>> matchedPatterns;
  private Pattern pattern;
  private int patternLength;

  public PatternMatcher(Pattern pattern)
  {
    this.pattern = pattern;
    patternLength = pattern.getPattern().size();
    matchedPatterns = Lists.newArrayList();
    for (int i = 0; i < patternLength; i++) {
      matchedPatterns.add(new ArrayList<T>());
    }
  }

  public transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      List<Integer> matchingPositions = pattern.getPosition(t);
      if (matchingPositions == null || matchingPositions.isEmpty()) {
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
          matchedPatterns.get(i-1).addAll(prev);
          prev.clear();
        }
        matchingPositionIndex++;
        if (matchingPositionIndex == matchingPositionLength) {
          i++;
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
      if (matchedPatterns.get(0) != null && !matchedPatterns.get(0).isEmpty()) {
        outputPort.emit(matchedPatterns.get(0));
        matchedPatterns.get(0).clear();
      }
    }
  };

  public transient DefaultOutputPort<List<T>> outputPort = new DefaultOutputPort<List<T>>();

  public static class Pattern<T>
  {
    private Map<T, List<Integer>> positionMap;
    private List<T> pattern;

    private Pattern()
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
      populatePositionMap();
    }
  }
}
