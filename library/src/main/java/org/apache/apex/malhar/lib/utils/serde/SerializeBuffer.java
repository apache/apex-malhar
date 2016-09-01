package org.apache.apex.malhar.lib.utils.serde;

import com.datatorrent.netlet.util.Slice;

public interface SerializeBuffer
{
  /**
   * write value. it could be part of the value
   * @param value
   */
  public void write(byte[] value);

  
  /**
   * write value. it could be part of the value
   * 
   * @param value
   * @param offset
   * @param length
   */
  public void write(byte[] value, int offset, int length);


  /**
   * set value and length. the input value is value only, it doesn't include
   * length information.
   * 
   * @param value
   * @param offset
   * @param length
   */
  public void setObjectByValue(byte[] value, int offset, int length);


  public void setObjectByValue(byte[] value);
  
  /**
   * reset the environment to reuse the resource.
   */
  public void reset();
  
  public void release();
  
  /**
   * This method should be called only the whole object has been written
   * @return The slice which represents the object
   */
  public Slice toSlice();
  
  
}
