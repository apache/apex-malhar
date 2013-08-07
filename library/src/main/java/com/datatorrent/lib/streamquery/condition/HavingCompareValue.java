package com.datatorrent.lib.streamquery.condition;

import java.util.ArrayList;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.streamquery.function.FunctionIndex;

@SuppressWarnings("rawtypes")
public class HavingCompareValue<T extends Comparable>   extends HavingCondition
{
  private T compareValue;
  private int compareType; 
  
  public HavingCompareValue(FunctionIndex aggregateIndex, T compareValue, int compareType)
  {
    super(aggregateIndex);
    this.compareValue = compareValue;
    this.compareType  = compareType;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean isValidAggregate(@NotNull ArrayList<Map<String, Object>> rows) throws Exception
  {
      Object computed = aggregateIndex.compute(rows);
      return (compareType == compareValue.compareTo(computed));
  }

}
