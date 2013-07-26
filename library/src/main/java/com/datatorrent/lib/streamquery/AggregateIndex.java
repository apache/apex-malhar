package com.datatorrent.lib.streamquery;

import java.util.ArrayList;
import java.util.Map;

abstract public class AggregateIndex 
{
  /**
   * Column/alias name.
   */
  protected String column = null;
  protected String alias = null;
  
  public AggregateIndex(String column, String alias) 
  {
    this.column = column;
    this.alias = alias;
  }
  
  abstract public Object compute(ArrayList<Map<String, Object>> rows);
  abstract protected String aggregateName();
  
  public void filter(ArrayList<Map<String, Object>> rows, Map<String, Object> collect)
  {
    if (rows == null) return;
    String name = column;
    if (alias != null) name = alias;
    if (name == null) name = aggregateName();
    collect.put(name, compute(rows));
  }
}
