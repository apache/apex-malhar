/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseColTupleGenerator extends BaseOperator implements InputOperator
{

  int colCount;

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<HBaseTuple> outputPort = new DefaultOutputPort<HBaseTuple>(this);

  @Override
  public void emitTuples()
  {
    HBaseTuple tuple = new HBaseTuple();
    tuple.setRow("row0");
    tuple.setColFamily("colfam0");
    tuple.setColName("col" + "-" +colCount);
    tuple.setColValue("val" + "-" + "0" + "-" + colCount);
    ++colCount;
    outputPort.emit(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    colCount = 0;
  }

}
