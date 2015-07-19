package com.datatorrent.contrib.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TupleGenerateCacheOperator<T> extends POJOTupleGenerateOperator<T>
{
  //one instance of TupleCacheOutputOperator map to one 
  private static Map< String, List<?> > emittedTuplesMap = new HashMap< String, List<?>>();

  private String uuid;
  
  public TupleGenerateCacheOperator()
  {
    uuid = java.util.UUID.randomUUID().toString();
  }
  
  @SuppressWarnings("unchecked")
  protected void tupleEmitted( T tuple )
  {
    List<T> emittedTuples = (List<T>)emittedTuplesMap.get(uuid);
    if( emittedTuples == null )
    {
      emittedTuples = new ArrayList<T>();
      emittedTuplesMap.put(uuid, emittedTuples);
    }
    emittedTuples.add(tuple);
  }
  
  @SuppressWarnings("unchecked")
  public List<T> getTuples()
  {
    return (List<T>)emittedTuplesMap.get(uuid);
  }
}