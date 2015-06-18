package com.datatorrent.lib.converter;


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * 
 * This operator converts Map<K, String> to Map<K, Number> for numeric string values
 *
 * @displayName String to Number value converter for Map
 * @category Converter
 * 
 */
public class StringValueToNumberConverterForMap<K> extends BaseOperator {

  /**
   * Input port which accepts Map<K, Numeric String>.
   */
  public final transient DefaultInputPort<Map<K, String>> input = new DefaultInputPort<Map<K, String>>()
  {
    @Override
    public void process(Map<K, String> tuple)
    {
      Map<K, Number> outputMap = new HashMap<K, Number>();
      for(Entry<K, String> entry:tuple.entrySet())
      {
        String val = entry.getValue();
        if (val == null) {
          return;
        }
        double tvalue = 0;
        boolean errortuple = false;
        try {
          tvalue = Double.parseDouble(val);
        }
        catch (NumberFormatException e) {
          errortuple = true;
        }
        if(!errortuple)
        {
          outputMap.put(entry.getKey(), tvalue);
        }
      }
      output.emit(outputMap);
    }

  };

  /*
   * Output port which outputs Map<K, Number> after converting numeric string to number
   */
  public final transient DefaultOutputPort<Map<K, Number>> output = new DefaultOutputPort<Map<K, Number>>();
}
