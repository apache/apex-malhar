package com.datatorrent.contrib.testhelper;

import com.datatorrent.api.BaseOperator;


public class CollectorModule<T> extends BaseOperator
{
  public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("collector", this);
}
