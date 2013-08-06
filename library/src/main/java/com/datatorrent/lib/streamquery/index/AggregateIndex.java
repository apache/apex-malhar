package com.datatorrent.lib.streamquery.index;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

abstract public class AggregateIndex 
{
  /**
   * Column/alias name.
   */
  protected String column = null;
  protected String alias = null;
  
  public AggregateIndex(String column, String alias) throws Exception
  {
    if (StringUtils.isEmpty(column)) {
      throw new Exception("Column can name can not be empty.");
    }
    this.column = column;
    this.alias = alias;
  }
  
  abstract public Object compute(ArrayList<Map<String, Object>> rows) throws Exception;
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
