/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseInputOperator<T> extends HBaseOperatorBase implements InputOperator
{

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);

  protected abstract T getTuple(Result result);

}
