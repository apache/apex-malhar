package com.datatorrent.lib.streamquery.function;

import java.util.ArrayList;
import java.util.Map;

import javax.validation.constraints.NotNull;

public class AverageFunction  extends FunctionIndex
{
  public AverageFunction(String column, String alias) throws Exception
  {
    super(column, alias);
  }

  @Override
  public Object compute(@NotNull ArrayList<Map<String, Object>> rows) throws Exception
  {
    if (rows.size() == 0) return 0.0;
    double sum = 0.0;
    for (Map<String, Object> row : rows) {
      sum += ((Number)row.get(column)).doubleValue();
    }
    return sum/rows.size();
  }

  @Override
  protected String aggregateName()
  {
    return "AVG(" + column + ")";
  }
}
