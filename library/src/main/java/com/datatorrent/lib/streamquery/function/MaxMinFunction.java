package com.datatorrent.lib.streamquery.function;

import java.util.ArrayList;
import java.util.Map;

import javax.validation.constraints.NotNull;

public class MaxMinFunction extends FunctionIndex
{
  private boolean isMax = true;
  
  public MaxMinFunction(@NotNull String column, @NotNull String alias, boolean isMin)
  {
    super(column, alias);
    isMax = !isMin;
  }

  @Override
  public Object compute(ArrayList<Map<String, Object>> rows) throws Exception
  {
    double minMax = 0.0;
    for (Map<String, Object> row : rows) {
      double value = ((Number)row.get(column)).doubleValue();
      if ((isMax && (minMax < value))||(!isMax && (minMax > value))) minMax = value;
     }
    return minMax;
  }

  @Override
  protected String aggregateName()
  {
    if (isMax) return "MAX(" + column + ")";
    return "MIN(" + column + ")";
  }

  public boolean isMax()
  {
    return isMax;
  }

  public void setMax(boolean isMax)
  {
    this.isMax = isMax;
  }

}
