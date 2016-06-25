package org.apache.apex.malhar.lib.window.impl;

import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The implementation of the Watermark tuple
 */
@InterfaceStability.Evolving
public class WatermarkImpl implements ControlTuple.Watermark
{
  private long timestamp;

  private WatermarkImpl()
  {
    // for kryo
  }

  public WatermarkImpl(long timestamp)
  {
    this.timestamp = timestamp;
  }

  @Override
  public long getTimestamp()
  {
    return timestamp;
  }

  @Override
  public String toString()
  {
    return "[Watermark " + getTimestamp() + "]";
  }
}
