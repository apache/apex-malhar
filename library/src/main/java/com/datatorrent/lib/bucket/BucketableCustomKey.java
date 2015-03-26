/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.bucket;

import java.util.ArrayList;

public class BucketableCustomKey implements Bucketable,Event
{
  protected ArrayList<Object> key;
  protected int size;
  protected String time;

  public ArrayList<Object> getKey()
  {
    return key;
  }

  public void setKey(ArrayList<Object> key)
  {
    this.key = key;
  }

  public int size()
  {
    size = key.size();
    return size;
  }

  @Override
  public Object getEventKey()
  {
    return key;
  }

  @Override
  public int hashCode()
  {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + 23 * Integer.parseInt(time);
    return result;
  }

 @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BucketableCustomKey)) {
      return false;
    }

    BucketableCustomKey temp = (BucketableCustomKey) o;

    if (time != temp.time) {
      return false;
    }
    if (key != null ? !key.equals(temp.key) : temp.key != null) {
      return false;
    }

    return true;
  }

  @Override
  public String getTime()
  {
    return time;
  }

  public void setTime(String time)
  {
    this.time = time;
  }

}
