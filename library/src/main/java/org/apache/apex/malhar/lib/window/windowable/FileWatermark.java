package org.apache.apex.malhar.lib.window.windowable;

import org.apache.apex.malhar.lib.window.WatermarkTuple;

public class FileWatermark implements WatermarkTuple
{
  private long timestamp;
  private DeliveryType deliveryType;
  private String fileName;

  public FileWatermark()
  {
  }

  public FileWatermark(long timestamp, DeliveryType type, String fileName)
  {
    this.timestamp = timestamp;
    this.deliveryType = type;
    this.fileName = fileName;
  }

  @Override
  public long getTimestamp()
  {
    return timestamp;
  }

  @Override
  public DeliveryType getDeliveryType()
  {
    return deliveryType;
  }

  public String getFileName()
  {
    return fileName;
  }

  public static class BeginFileWatermark extends FileWatermark
  {
    public BeginFileWatermark()
    {
    }
    public BeginFileWatermark(long timestamp, String fileName)
    {
      super(timestamp, DeliveryType.IMMEDIATE, fileName);
    }
  }

  public static class EndFileWatermark extends FileWatermark
  {
    public EndFileWatermark()
    {
    }
    public EndFileWatermark(long timestamp, String fileName)
    {
      super(timestamp, DeliveryType.END_WINDOW, fileName);
    }
  }
}
