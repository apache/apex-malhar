package com.datatorrent.lib.window;

import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public abstract class WindowOption
{

  private TriggerOption[] triggerOptions;

  private AccumulationMode accumulationMode = AccumulationMode.ACCUMULATE;

  private List<Object[]> lateness = new LinkedList<>();

  public static enum AccumulationMode
  {
    DISCARD,
    ACCUMULATE,
    ACCUMULATE_DELTA
  }

  public static class GlobalWindow extends WindowOption
  {

  }

  public static class FixedWindow extends WindowOption
  {
    FixedWindow(long quantity, Quantification.Unit unit)
    {
      size.add(new Object[]{quantity, unit});
    }

    FixedWindow(List<Object[]> size)
    {
      this.size = size;
    }

    private List<Object[]> size = new LinkedList<>();

    public SlidingWindow slideBy(long slidingQuantity, Quantification.Unit slidingUnit)
    {
      SlidingWindow sw = new SlidingWindow(this.size);
      sw.delta.add(new Object[]{slidingQuantity, slidingUnit});
      return sw;
    }

    public FixedWindow and(long quantity, Quantification.Unit unit)
    {
      size.add(new Object[]{quantity, unit});
      return this;
    }

    public List<Object[]> getSize()
    {
      return size;
    }
  }

  public static class SlidingWindow extends FixedWindow
  {

    SlidingWindow(List<Object[]> size)
    {
      super(size);
    }

    List<Object[]> delta = new LinkedList<>();

    @Override
    public SlidingWindow and(long quantity, Quantification.Unit unit)
    {
      delta.add(new Object[]{quantity, unit});
      return this;
    }
  }

  public static class SessionWindow extends WindowOption
  {

    SessionWindow(long quantity, Quantification.Unit unit)
    {
      gap.add(new Object[]{quantity, unit});
    }

    List<Object[]> gap = new LinkedList<>();

    public SessionWindow and(long quantity, Quantification.Unit unit)
    {
      gap.add(new Object[]{quantity, unit});
      return this;
    }

  }

  public static class WindowOptionBuilder
  {

    public static FixedWindow intoEvery(long quantity, Quantification.Unit unit)
    {
      return new FixedWindow(quantity, unit);
    }

    public static GlobalWindow all()
    {
      return new GlobalWindow();
    }

    public static SessionWindow allTuplesWithin(long quatity, Quantification.Unit unit)
    {
      return new SessionWindow(quatity, unit);
    }

  }



  // Pre-defined trigger emit tuple
  public WindowOption emitUpdate(TriggerOption... options)
  {
    triggerOptions = options;
    return this;
  }

  public WindowOption setAccumulationMode(AccumulationMode accumulationMode)
  {
    this.accumulationMode = accumulationMode;
    return this;
  }

  public WindowOption withLateness(Object[]... lateness)
  {
    this.lateness.add(lateness);
    return this;
  }




}
