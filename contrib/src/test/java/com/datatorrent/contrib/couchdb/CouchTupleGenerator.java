package com.datatorrent.contrib.couchdb;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Generates Map objects which can be saved in couchdb<br></br>
 *
 * @since 0.3.5
 */
public class CouchTupleGenerator extends BaseOperator implements InputOperator
{

  private transient int count = 1;

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<Map<Object, Object>> outputPort = new DefaultOutputPort<Map<Object, Object>>();

  @Override
  public void emitTuples()
  {
    Map<Object, Object> tuple = Maps.newHashMap();
    tuple.put("_id", "TestDatabase");
    tuple.put("name", "CouchDBGenerator");
    tuple.put("type", "Output");
    tuple.put("value", Integer.toString(count++));

    outputPort.emit(tuple);
  }
}
