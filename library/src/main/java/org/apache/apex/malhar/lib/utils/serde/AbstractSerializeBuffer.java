package org.apache.apex.malhar.lib.utils.serde;

import com.datatorrent.netlet.util.Slice;

public abstract class AbstractSerializeBuffer implements SerializeBuffer, ResetableWindowListener
{
  protected WindowableByteStream windowableByteStream;
  
  /**
   * write value. it could be part of the value
   * @param value
   */
  @Override
  public void write(byte[] value)
  {
    windowableByteStream.write(value);
  }
  
  /**
   * write value. it could be part of the value
   * 
   * @param value
   * @param offset
   * @param length
   */
  @Override
  public void write(byte[] value, int offset, int length)
  {
    windowableByteStream.write(value, offset, length);
  }


  @Override
  public void setObjectByValue(byte[] value)
  {
    setObjectByValue(value, 0, value.length);
  }

  public long size()
  {
    return windowableByteStream.size();
  }
  
  public long capacity()
  {
    return windowableByteStream.capacity();
  }

  /**
   * This method should be called only the whole object has been written
   * @return The slice which represents the object
   */
  public Slice toSlice()
  {
    return windowableByteStream.toSlice();
  }


  /**
   * reset the environment to reuse the resource.
   */
  public void reset()
  {
    windowableByteStream.reset();
  }
  

  @Override
  public void beginWindow(long windowId)
  {
    windowableByteStream.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    windowableByteStream.endWindow();    
  }
  
  /**
   * reset for all windows which window id less or equal input windowId
   * this interface doesn't enforce to call reset window for each windows. Several windows can be reset at the same time.
   * @param windowId
   */
  public void resetUpToWindow(long windowId)
  {
    windowableByteStream.resetUpToWindow(windowId);
  }

  public void release()
  {
    reset();
    windowableByteStream.release();
  }
  
  public WindowableByteStream createWindowableByteStream()
  {
    return new WindowableBlocksStream();
  }

  public WindowableByteStream createWindowableByteStream(int capacity)
  {
    return new WindowableBlocksStream(capacity);
  }
}
