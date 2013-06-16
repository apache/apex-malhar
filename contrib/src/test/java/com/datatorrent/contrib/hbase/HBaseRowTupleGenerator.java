/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.hbase;

import com.datatorrent.lib.io.SimpleSinglePortInputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

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
