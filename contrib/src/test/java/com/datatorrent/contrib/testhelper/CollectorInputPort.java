package com.datatorrent.contrib.testhelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

public class CollectorInputPort<T> extends DefaultInputPort<T>
{
  public static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();
  ArrayList<T> list;

  final String id;

  public CollectorInputPort(String id, Operator module)
  {
    super();
    this.id = id;
  }

  @Override
  public void process(T tuple)
  {
//    System.out.print("collector process:"+tuple);
    list.add(tuple);
  }

  @Override
  public void setConnected(boolean flag)
  {
    if (flag) {
      collections.put(id, list = new ArrayList<T>());
    }
  }
}