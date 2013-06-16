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
import com.malhartech.lib.io.SimpleSinglePortInputOperator;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseRowTupleGenerator extends BaseOperator implements InputOperator
{

  int rowCount;

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<HBaseTuple> outputPort = new DefaultOutputPort<HBaseTuple>(this);

  @Override
  public void emitTuples()
  {
    HBaseTuple tuple = new HBaseTuple();
    tuple.setRow("row" + rowCount);
    tuple.setColFamily("colfam0");
    tuple.setColName("col" + "-" +0);
    tuple.setColValue("val" + "-" + rowCount + "-" + 0);
    ++rowCount;
    outputPort.emit(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    rowCount = 0;
  }

}
