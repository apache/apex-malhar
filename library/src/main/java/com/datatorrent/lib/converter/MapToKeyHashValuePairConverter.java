package com.datatorrent.lib.converter;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyHashValPair;
import com.datatorrent.lib.util.KeyValPair;

/**
 * 
 * This operator outputs key value pair for each entry in input Map 
 * 
 * @displayName Map to key-value pair converter
 * @category Converter
 * 
 */
public class MapToKeyHashValuePairConverter<K, V> extends BaseOperator {

Map<K, Number> outputMap = new HashMap<K, Number>();
  
  /**
   * Input port which accepts Map<K, V>.
   */
  public final transient DefaultInputPort<Map<K, V>> input = new DefaultInputPort<Map<K, V>>()
  {
    @Override
    public void process(Map<K, V> tuple)
    { 
      outputMap.clear();
      for(Entry<K, V> entry:tuple.entrySet())
      {
        output.emit(new KeyHashValPair<K, V>(entry.getKey(), entry.getValue()));  
      }      
    }
  };

  /*
   * Output port which outputs KeyValue pair for each entry in Map
   */
  public final transient DefaultOutputPort<KeyHashValPair<K, V>> output = new DefaultOutputPort<KeyHashValPair<K, V>>();
}
