package com.datatorrent.lib.window;

import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public class TriggerOption
{

  public static enum WatermarkOpt
  {
    BEFORE,
    AFTER
  }

  WatermarkOpt watermarkOpt = WatermarkOpt.BEFORE;

  private List<Object[]> size = new LinkedList<>();

  public static class TriggerOptionBuilder
  {

    public static TriggerOption every(long quantity, Quantification.Unit unit)
    {
      TriggerOption opt = new TriggerOption();
      opt.size.add(new Object[]{quantity, unit});
      return opt;
    }
  }


  public TriggerOption and(long quantity, Quantification.Unit unit)
  {
    size.add(new Object[]{quantity, unit});
    return this;
  }

  public TriggerOption is(WatermarkOpt watermarkOpt)
  {
    this.watermarkOpt = watermarkOpt;
    return this;
  }

  public List<Object[]> getSize()
  {
    return size;
  }

  public WatermarkOpt getWatermarkOpt()
  {
    return watermarkOpt;
  }
}
