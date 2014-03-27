package com.datatorrent.lib.datamodel.operation;

public class SumOperation<OUTPUT extends Number, INPUT extends Number> implements Operation<OUTPUT, INPUT>
{

  @SuppressWarnings("unchecked")
  @Override
  public OUTPUT compute(OUTPUT last, INPUT value)
  {
    if (last instanceof Double) {
      return (OUTPUT) (new Double(last.doubleValue() + value.doubleValue()));
    }
    else if (last instanceof Float) {
      return (OUTPUT) (new Float(last.floatValue() + value.floatValue()));
    }
    else if (last instanceof Long) {
      return (OUTPUT) (new Long(last.longValue() + value.longValue()));
    }
    else {
      return (OUTPUT) (new Integer(last.intValue() + value.intValue()));
    }
  }

}
