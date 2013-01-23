/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.singlejoin;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class SingleJoinBolt extends BaseOperator
{
  private HashMap<Integer, Object> map1 = new HashMap<Integer, Object>();
  private HashMap<Integer, Object> map2 = new HashMap<Integer, Object>();
  public transient DefaultInputPort<ArrayList<Object>> age = new DefaultInputPort<ArrayList<Object>>(this)
  {
    @Override
    public void process(ArrayList<Object> tuple)
    {
      Integer id = (Integer)tuple.get(0);
      Integer age = (Integer)tuple.get(1);
      if( map2.containsKey(id) ) {
        String name = (String)map2.remove(id);
        ArrayList list = new ArrayList<Object>();
        list.add(age);
        list.add(name);
        output.emit(list);
      }
      else {
        if( !map1.containsKey(id) ) {
          map1.put(id, age);
        }
      }
    }
  };
  
  public transient DefaultInputPort<ArrayList<Object>> name = new DefaultInputPort<ArrayList<Object>>(this)
  {
    @Override
    public void process(ArrayList<Object> tuple)
    {
      Integer id = (Integer)tuple.get(0);
      String name = (String)tuple.get(1);
      if( map1.containsKey(id) ) {
        Integer age = (Integer)map1.remove(id);
        ArrayList list = new ArrayList<Object>();
        list.add(age);
        list.add(name);
        output.emit(list);
      }
      else {
        if( !map2.containsKey(id) ) {
          map2.put(id, name);
        }
      }
    }
  };

  public transient DefaultOutputPort<ArrayList<Object>> output = new DefaultOutputPort<ArrayList<Object>>(this);
}
