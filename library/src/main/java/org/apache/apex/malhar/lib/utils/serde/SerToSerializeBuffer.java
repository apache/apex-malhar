package org.apache.apex.malhar.lib.utils.serde;

import com.datatorrent.netlet.util.Slice;

public interface SerToSerializeBuffer<T> extends Serde<T, Slice>
{
  void serTo(T object, SerializeBuffer buffer);
  
  void reset();
}
