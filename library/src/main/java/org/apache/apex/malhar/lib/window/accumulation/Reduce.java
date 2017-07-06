package org.apache.apex.malhar.lib.window.accumulation;

import org.apache.apex.malhar.lib.window.Accumulation;

public interface Reduce<T> extends Accumulation<T,T,T>
{
  public T reduce(T input1, T input2);
}
