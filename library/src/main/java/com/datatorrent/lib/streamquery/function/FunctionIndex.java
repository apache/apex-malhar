package com.datatorrent.lib.streamquery.function;

import java.util.ArrayList;
import java.util.Map;

import javax.validation.constraints.NotNull;

abstract public class FunctionIndex 
{
  /**
   * Column/alias name.
   */
  @NotNull
  protected String column;
  protected String alias;
  
  public FunctionIndex(@NotNull String column, String alias) 
  {
    this.column = column;
    this.alias = alias;
  }
  
  abstract public Object compute(@NotNull ArrayList<Map<String, Object>> rows) throws Exception;
  abstract protected String aggregateName();
  
  public void filter(ArrayList<Map<String, Object>> rows, Map<String, Object> collect) throws Exception
  {
    if (rows == null) return;
    String name = column;
    if (alias != null) name = alias;
    if (name == null) name = aggregateName();
    collect.put(name, compute(rows));
  }
}
