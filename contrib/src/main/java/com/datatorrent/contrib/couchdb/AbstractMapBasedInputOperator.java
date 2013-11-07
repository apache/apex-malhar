package com.datatorrent.contrib.couchdb;

import com.google.common.collect.Maps;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * <br>Couch-db input adaptor that emits a map.</br>
 * <br>This adaptor emits the result of a database view in form of a map.</br>
 * <br>It uses the emitTuples implementation of {@link AbstractCouchDBInputOperator} which emits the complete result
 * of the ViewQuery every window cycle. </br>
 * @since 0.3.5
 */
public abstract class AbstractMapBasedInputOperator extends AbstractCouchDBInputOperator<Map<Object, Object>>
{
  private transient ObjectMapper mapper;

  public AbstractMapBasedInputOperator()
  {
    mapper = new ObjectMapper();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<Object, Object> getTuple(JsonNode value)
  {
    Map<Object, Object> valueMap = Maps.newHashMap();
    try {
      valueMap = mapper.readValue(value, valueMap.getClass());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return valueMap;
  }
}
