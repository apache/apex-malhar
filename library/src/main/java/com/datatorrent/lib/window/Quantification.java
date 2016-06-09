package com.datatorrent.lib.window;

/**
 * Created by siyuan on 6/3/16.
 */
public class Quantification
{
  public static interface Unit
  {
  }

  public static enum CountableUnit implements Unit
  {
    TUPLE,
    KTUPLE,
    MTUPLE
  }

  public static enum TimeUnit implements Unit
  {
    MILLISECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    WEEK,
    MONTH,
    YEAR
  }

}
