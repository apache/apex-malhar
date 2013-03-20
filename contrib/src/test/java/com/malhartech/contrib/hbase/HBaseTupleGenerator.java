/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import com.malhartech.lib.io.SimpleSinglePortInputOperator;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseTupleGenerator extends BaseOperator implements InputOperator
{

  int rowCount;

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<HBaseTuple> outputPort = new DefaultOutputPort<HBaseTuple>(this);

  @Override
  public void emitTuples()
  {
    HBaseTuple tuple = new HBaseTuple();
    tuple.setRow("row" + rowCount);
    tuple.setColFamily("cf1");
    tuple.setCol1Value("val" + rowCount + "-" + 1);
    tuple.setCol2Value("val" + rowCount + "-" + 2);
    rowCount++;
    outputPort.emit(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    rowCount = 0;
  }

}
