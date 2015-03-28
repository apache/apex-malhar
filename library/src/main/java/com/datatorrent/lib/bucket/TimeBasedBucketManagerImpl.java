/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.bucket;


public class TimeBasedBucketManagerImpl<T extends Event & Bucketable> extends AbstractTimeBasedBucketManagerImpl<T>
{

  @Override
  protected long getTime(T event)
  {
   return  event.getTime();
  }

  @Override
  protected Bucket<T> createBucket(long requestedKey)
  {
    return new Bucket<T>(requestedKey);
  }

  @Override
  protected Object getEventKey(T event)
  {
    return event.getEventKey();
  }

  @Override
  protected TimeBasedBucketManagerImpl<T> getBucketManagerImpl()
  {
    return new TimeBasedBucketManagerImpl<T>();
  }



}
