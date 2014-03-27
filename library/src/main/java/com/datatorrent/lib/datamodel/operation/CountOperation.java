package com.datatorrent.lib.datamodel.operation;

public class CountOperation<OUTPUT extends Number> implements Operation<OUTPUT, OUTPUT>
{

  @Override
  public OUTPUT compute(OUTPUT last, OUTPUT value)
  {
    if (last instanceof Double) {
      return (OUTPUT) (new Double(last.doubleValue() + 1));
    }
    else if (last instanceof Float) {
      return (OUTPUT) (new Float(last.floatValue() + 1));
    }
    else if (last instanceof Long) {
      return (OUTPUT) (new Long(last.longValue() + 1));
    }
    else {
      return (OUTPUT) (new Integer(last.intValue() + 1));
    }
  }

}
