package com.datatorrent.contrib.couchdb;

import com.google.common.collect.Maps;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * <br>Couch-db input adaptor that emits a map</br>
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
  public Map<Object, Object> getTuple(String id, JsonNode value)
  {
    Map<Object, Object> mapTuple = Maps.newHashMap();
    mapTuple.put("_id", id);
    Map<Object, Object> valueMap = Maps.newHashMap();
    try {
      valueMap = mapper.readValue(value, valueMap.getClass());
    } catch (IOException e) {
      e.printStackTrace();
    }
    mapTuple.putAll(valueMap);
    return mapTuple;
  }
}
