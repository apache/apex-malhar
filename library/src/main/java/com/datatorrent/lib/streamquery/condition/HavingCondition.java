package com.datatorrent.lib.streamquery.condition;

import java.util.Map;

import com.datatorrent.lib.streamquery.function.FunctionIndex;

public abstract class HavingCondition
{
  FunctionIndex  aggregateIndex = null;
  public HavingCondition(FunctionIndex  aggregateIndex) {
    this.aggregateIndex = aggregateIndex;
  }
  
  abstract public boolean isValidAggregate(Map<String, Object> row);
}
