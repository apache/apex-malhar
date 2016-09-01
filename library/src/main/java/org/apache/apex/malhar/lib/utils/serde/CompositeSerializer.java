package org.apache.apex.malhar.lib.utils.serde;

import javax.validation.constraints.NotNull;

import com.datatorrent.netlet.util.Slice;

public class CompositeSerializer<T1, T2>
{
  protected SerToSerializeBuffer<T1> serializer1;
  protected SerToSerializeBuffer<T2> serializer2;
  
  @NotNull
  protected LengthValueBuffer buffer;
  
  //for Kyro
  protected CompositeSerializer()
  {
  }

  public CompositeSerializer(SerToSerializeBuffer<T1> serializer1, SerToSerializeBuffer<T2> serializer2, @NotNull LengthValueBuffer buffer)
  {
    this.serializer1 = serializer1;
    this.serializer2 = serializer2;
    this.buffer = buffer;
  }
  
  public Slice serialize(T1 value1, T2 value2)
  {
    serializeFirst(value1);
    serializeSecond(value2);
    return buffer.toSlice();
  }
  
  protected void serializeFirst(T1 value1)
  {
    serializer1.serTo(value1, buffer);
  }
  
  protected void serializeSecond(T2 value2)
  {
    serializer2.serTo(value2, buffer);
  }
}
