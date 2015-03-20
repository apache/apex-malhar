/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.bucket;

import java.util.ArrayList;


public class DummyCustomEvent extends BucketableCustomKey implements Event//, Comparable<DummyCustomEvent>
{
  ArrayList<String> id;
  long time;

  @SuppressWarnings("unused")
  DummyCustomEvent()
  {
  }

  public DummyCustomEvent(ArrayList<String> id, long time)
  {
    this.id = id;
    this.time = time;
  }

  @Override
  public long getTime()
  {
    return time;
  }


  /* @Override
  public int compareTo(@Nonnull DummyCustomEvent dummyEvent)
  {
    return id - dummyEvent.id;
  }*/

   @Override
  public String toString()
  {
    return "{id=" + id + ", time=" + time + '}';
  }

}
