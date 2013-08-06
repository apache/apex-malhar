package com.datatorrent.lib.streamquery.condition;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

public class InCondition extends Condition
{
  @NotNull
  private String column;
  private Set<Object> inValues = new HashSet<Object>();
  
  public InCondition(@NotNull String column) {
    this.column = column;
  }
  
  @Override
  public boolean isValidRow(@NotNull Map<String, Object> row)
  {
    if (!row.containsKey(column)) return false;
    return inValues.contains(row.get(column));
  }

  @Override
  public boolean isValidJoin(@NotNull Map<String, Object> row1, @NotNull Map<String, Object> row2)
  {
    return false;
  }

  public String getColumn()
  {
    return column;
  }

  public void setColumn(String column)
  {
    this.column = column;
  }
  
  public void addInValue(Object value) {
    this.inValues.add(value);
  }
   
}
