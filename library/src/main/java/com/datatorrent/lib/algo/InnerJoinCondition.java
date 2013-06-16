package com.datatorrent.lib.algo;

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.AbstractBaseMatchOperator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * <b>Not certified yet</b>
 * Takes two streams, and emits innerJoin result as per Key<p>
 * <br>
 * Tuples with same value for "key" are merged into one tuple. Even though this module produces continuous tuples, at end of window all data is flushed. Thus the data set is windowed
 * and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data1</b>: expects HashMap<K,V><br>
 * <b>data2</b>: expects HashMap<K,V><br>
 * <b>result</b>: emits HashMap<K,V><br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key to "joinby"<br>
 * <b>filter1</b>: The keys from port data1 to include<br>
 * <b>filter2</b>: The keys from port data2 to include<br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <b>key</b> cannot be empty<br>
 * <br>
 * Run time checks are:<br>
 * All incoming tuples must include the key to groupby
 * <br>
 *
 * @author amol<br>
 *
 */
public class InnerJoinCondition<K, V extends Comparable> extends AbstractBaseMatchOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data1")
  public final transient DefaultInputPort<HashMap<K, V>> data1 = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Checks if key exists. If so emits all current combinations with matching tuples received on port "data2"
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      V val = tuple.get(getKey());
      if (val == null) { // emit error tuple
        return;
      }
      if (!matchCondition(val)) {
        return;
      }
      emitTuples(tuple, map2.get(val), val, filter1);
      registerTuple(tuple, map1, val, filter1);
    }
  };
  @InputPortFieldAnnotation(name = "data2")
  public final transient DefaultInputPort<HashMap<K, V>> data2 = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Checks if key exists. If so emits all current combinations with matching tuples received on port "data1"
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      V val = tuple.get(getKey());
      if (val == null) { // emit error tuple
        return;
      }
      if (!matchCondition(val)) {
        return;
      }
      emitTuples(tuple, map1.get(val), val, filter2);
      registerTuple(tuple, map2, val, filter2);
    }
  };
  @OutputPortFieldAnnotation(name = "result")
  public final transient DefaultOutputPort<HashMap<K, V>> result = new DefaultOutputPort<HashMap<K, V>>(this);

  /**
   * Adds tuples to the list associated with its port
   *
   * @param tuple
   * @param map
   * @param val
   */
  protected void registerTuple(HashMap<K, V> tuple,
          HashMap<V, ArrayList<HashMap<K, V>>> map,
          V val,
          HashMap<K, Object> filter)
  {
    // Construct the data (HashMap) to be inserted into sourcemap
    HashMap<K, V> data = new HashMap<K, V>();
    for (Map.Entry<K, Object> e: filter.entrySet()) {
      if (tuple.containsKey(e.getKey())) {
        data.put(cloneKey(e.getKey()), cloneValue(tuple.get(e.getKey())));
      }
    }
    ArrayList<HashMap<K, V>> list = map.get(val);
    if (list == null) {
      list = new ArrayList<HashMap<K, V>>();
      map.put(val, list);
    }
    list.add(data);
  }
  HashMap<K, Object> filter1 = new HashMap<K, Object>();
  HashMap<K, Object> filter2 = new HashMap<K, Object>();
  protected HashMap<V, ArrayList<HashMap<K, V>>> map1 = new HashMap<V, ArrayList<HashMap<K, V>>>();
  protected HashMap<V, ArrayList<HashMap<K, V>>> map2 = new HashMap<V, ArrayList<HashMap<K, V>>>();

  /**
   * Sets keys to filter
   *
   * @param list list of keys on port data1 to add to the result
   */
  public void setFilter1(ArrayList<K> list)
  {
    for (K o: list) {
      filter1.put(o, null);
    }
  }

  /**
   * Sets keys to filter
   *
   * @param list list of keys on port data2 to add to the result
   */
  public void setFilter2(ArrayList<K> list)
  {
    for (K o: list) {
      filter2.put(o, null);
    }
  }

  /**
   * Checks that the two filter lists do not have identical keys
   * Inner join with same column name not yet supported
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    for (Map.Entry<K, Object> o: filter1.entrySet()) {
      if (filter2.containsKey(o.getKey())) {
        throw new IllegalArgumentException(String.format("Key \"%s\" exist on both filters", o.getKey().toString()));
      }
    }
  }

  /**
   * Clears cache/hash for both ports
   */
  @Override
  public void endWindow()
  {
    map1.clear();
    map2.clear();
  }



  /**
   * Emits all combinations of source and matching other list
   * @param row to emit on
   * @param list of other table/stream
   * @param val value of the key
   * @param filter filters keys from row
   */
  public void emitTuples(HashMap<K, V> row,
          ArrayList<HashMap<K, V>> list,
          V val,
          HashMap<K, Object> filter)
  {
    if (list == null) { // The currentList does not have the value yet
      return;
    }

    HashMap<K, V> tuple;
    for (HashMap<K, V> e: list) {
      tuple = new HashMap<K, V>();
      tuple.put(getKey(), val);
      for (Map.Entry<K, V> o: e.entrySet()) {
        tuple.put(cloneKey(o.getKey()), cloneValue(o.getValue()));
      }
      for (Map.Entry<K, Object> o: filter.entrySet()) {
        V rval = row.get(o.getKey());
        if (rval != null) {
          tuple.put(cloneKey(o.getKey()), cloneValue(rval));
        }
      }
      result.emit(tuple);
    }
  }
}
