/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.bucket;

import java.util.ArrayList;

public class BucketableCustomKey implements Bucketable
{
  private ArrayList<Object> key;
  Integer id;

  public Integer getId()
  {
    return id;
  }

  public void setId(Integer id)
  {
    this.id = id;
  }

  public ArrayList<Object> getKey()
  {
    return key;
  }

  public void setKey(ArrayList<Object> key)
  {
    this.key = key;
  }

  @Override
  public Object getEventKey()
  {
    return key;
  }

  @Override
  public int hashCode()
  {
    int result = 31;
    for (Object key1: key) {
      result = 23 * result + key1.hashCode();
    }
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

    BucketableCustomKey customKey = (BucketableCustomKey) o;

    if (key != null ? !key.equals(customKey.key) : customKey.key != null) {
      return false;
    }

    if(key!=null)
    {
      if(key.size() == customKey.key.size())
      {
         for(int i=0;i<key.size();i++)
         {
           if(!key.get(i).equals(customKey.key.get(i)))
             return false;
         }
         return true;
      }
      else
        return false;
    }
    else if(customKey.key!=null)
    {
      return false;
    }

    return true;
  }

}
