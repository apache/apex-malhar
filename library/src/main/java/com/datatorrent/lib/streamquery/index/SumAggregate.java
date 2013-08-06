package com.datatorrent.lib.streamquery.index;

import java.util.ArrayList;
import java.util.Map;


public class SumAggregate extends AggregateIndex
{
  public SumAggregate(String column, String alias) throws Exception
  {
    super(column, alias);
  }

  @Override
  public Object compute(ArrayList<Map<String, Object>> rows) throws Exception
  {
    Double result = 0.0;
    for (Map<String, Object> row : rows) {
        if (!row.containsKey(column)) continue;
        result += ((Number)row.get(column)).doubleValue();
    }
    return result;
  }

  @Override
  protected String aggregateName()
  {
   return "Sum(" + column;
  }

}
