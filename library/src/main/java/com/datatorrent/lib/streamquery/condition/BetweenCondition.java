package com.datatorrent.lib.streamquery.condition;

import java.util.Map;

import javax.validation.constraints.NotNull;

public class BetweenCondition  extends Condition
{
  @NotNull  
  private String column;
  @NotNull
  private Object leftValue;
  @NotNull
  private Object rightValue;
  
  public BetweenCondition(@NotNull String column, @NotNull  Object leftValue, @NotNull Object rightValue) 
  {
    this.column = column;
    this.leftValue = leftValue;
    this.rightValue = rightValue;
  }
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public boolean isValidRow(@NotNull Map<String, Object> row)
  {
    if (!row.containsKey(column)) return false;
    Object value = row.get(column);
    if (value == null) return false;
    if (((Comparable)value).compareTo((Comparable)leftValue) < 0) return false;
    if (((Comparable)value).compareTo((Comparable)rightValue) > 0) return false;
    return true;
  }

  @Override
  public boolean isValidJoin(@NotNull Map<String, Object> row1, Map<String, Object> row2)
  {
    // TODO Auto-generated method stub
    return false;
  }

}
