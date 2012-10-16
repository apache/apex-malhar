/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes two streams via input port "in_data1" and "in_data2", and outputs GroupBy property "Key" on output port out_data<p>
 * <br>
 * Even though this module produces continuous tuples, at end of window all data is flushed. Thus the data set is windowed
 * and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>in_data1</b>: Input data port expects HashMap<String, Object><br>
 * <b>in_data2</b>: Input data port expects HashMap<String, Object><br>
 * <b>out_data</b>: Output data port, emits HashMap<String, Object><br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key to "groupby"<br>
 *
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <b>key</b> cannot be empty<br>
 * <br>
 * Run time checks are:<br>
 * All incoming tuples must include the groupby key.
 *
 *
 * @author amol<br>
 *
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = GroupBy.IPORT_IN_DATA1, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = GroupBy.IPORT_IN_DATA2, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = GroupBy.OPORT_OUT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class GroupBy extends AbstractModule
{
  private static Logger LOG = LoggerFactory.getLogger(GroupBy.class);
  public static final String IPORT_IN_DATA1 = "in_data1";
  public static final String IPORT_IN_DATA2 = "in_data2";
  public static final String OPORT_OUT_DATA = "out_data";


  String groupby = null;
  HashMap<Object, Object> map1 = new HashMap<Object, Object>();
  HashMap<Object, Object> map2 = new HashMap<Object, Object>();

  /**
   * The group by key
   *
   */
  public static final String KEY_GROUPBY = "groupby";


  @Override
  public void beginWindow()
  {
    map1.clear();
    map2.clear();
  }

  public void emitTuples(HashMap<String, Object> source, Object currentList, Object val) {
    if (currentList == null) { // The currentList does not have the value yet
      return;
    }

    ArrayList<HashMap<String, Object>> list = (ArrayList<HashMap<String, Object>>) currentList;
    HashMap<String, Object> tuple;
    for (HashMap<String, Object> e : list) {
      tuple = new HashMap<String, Object>();
      tuple.put(groupby, val);
      for (Map.Entry<String, Object> o : e.entrySet()) {
        tuple.put(o.getKey(), o.getValue());
      }
      for (Map.Entry<String, Object> o : source.entrySet()) {
        if (!o.getKey().equals(groupby)) {
          tuple.put(o.getKey(), o.getValue());
        }
      }
       emit(tuple);
    }
  }

  /**
   *
   * Takes in a key and an arrayIndex. ReverseIndexes the strings in the ArrayIndex
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    Object val = ((HashMap<String, Object>) payload).get(groupby);
    if (val == null) { // emit error tuple
      return;
    }
    boolean prt1 = IPORT_IN_DATA1.equals(getActivePort());
    HashMap<Object, Object> sourcemap = prt1 ? map1 : map2;
    HashMap<Object, Object> othermap = prt1 ? map2 : map1;

    // emit tuples with the other source.
    emitTuples((HashMap<String, Object>) payload, othermap.get(val), val);

    // Construct the data (HashMap) to be inserted into sourcemap
    HashMap<String, Object> data = new HashMap<String, Object>();
    for (Map.Entry<String, Object> e: ((HashMap<String, Object>)payload).entrySet()) {
        if (!e.getKey().equals(groupby)) {
          data.put(e.getKey(), e.getValue());
        }
    }

    ArrayList<HashMap<String, Object>> list = (ArrayList<HashMap<String, Object>>) sourcemap.get(val);
    if (list == null) {
      list = new ArrayList<HashMap<String, Object>>();
      sourcemap.put(val, list);
    }
    list.add(data);
  }

  /**
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;
    groupby = config.get(KEY_GROUPBY);

    if ((groupby == null) || (groupby.isEmpty())) {
      ret = false;
      throw new IllegalArgumentException(String.format("Parameter \"%s\" is empty", KEY_GROUPBY));
    }
    return ret;
  }

  /**
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }
    groupby = config.get(KEY_GROUPBY);
    LOG.debug(String.format("Set up: \"groupby\" key set to %s", groupby));
  }
}
