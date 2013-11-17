package com.datatorrent.demos.scalability;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;

/**
 * <p>AdsAggregationOperator class.</p>
 *
 * @since 0.9.0
 */
public class AdsAggregationOperator implements Unifier<KeyHashValPair<Map<String, MutableDouble>>>
{
  private static final Logger logger = LoggerFactory.getLogger(AdsAggregationOperator.class);
  protected Map<AggrKey, Map<String, MutableDouble>> dataMap = new HashMap<AggrKey, Map<String, MutableDouble>>();

  public final transient DefaultOutputPort<KeyHashValPair<Map<String, MutableDouble>>> output = new DefaultOutputPort<KeyHashValPair<Map<String, MutableDouble>>>();

  @Override
  public void process(KeyHashValPair<Map<String, MutableDouble>> t)
  {
    Map<String, MutableDouble> value = t.getValue();
    AggrKey key = t.getKey();
    Map<String, MutableDouble> map = dataMap.get(key);
    if (map == null) {
      map = new HashMap<String, MutableDouble>();
      dataMap.put(key, map);
    }

    for (Map.Entry<String, MutableDouble> entry1 : value.entrySet()) {
      String field = entry1.getKey();
      Number oldVal = (Number) map.get(field);
      if (oldVal == null) {
        map.put(field, convertToNumber(entry1.getValue()));
      } else if (oldVal instanceof MutableDouble) {
        ((MutableDouble) oldVal).add(convertToNumber(entry1.getValue()));
      } else if (oldVal instanceof MutableLong) {
        ((MutableLong) oldVal).add(convertToNumber(entry1.getValue()));
      } else {
        throw new RuntimeException("Values of unexpected type in data map value field type. Expecting MutableLong or MutableDouble");
      }
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void beginWindow(long windowId)
  {
    dataMap = new HashMap<AggrKey, Map<String, MutableDouble>>();
  }

  @Override
  public void endWindow()
  {
    if (dataMap != null) {
      for (Map.Entry<AggrKey, Map<String, MutableDouble>> entry : dataMap.entrySet()) {
        output.emit(new KeyHashValPair<Map<String, MutableDouble>>(entry.getKey(), entry.getValue()));
      }
      dataMap.clear();
    }
  }

  protected MutableDouble convertToNumber(Object o)
  {
    if (o == null) {
      return null;
    } else if (o instanceof MutableDouble || o instanceof MutableLong) {
      return (MutableDouble) o;
    } else if (o instanceof Double || o instanceof Float) {
      return new MutableDouble((Number) o);
    } else if (o instanceof Number) {
      return new MutableDouble((Number) o);
    } else {
      return new MutableDouble(o.toString());
    }
  }

}
