/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.bucket.NonOperationalBucketStore;
import com.datatorrent.lib.dedup.AbstractDeduper;
import com.google.common.collect.Lists;
import java.util.concurrent.Exchanger;

/**
 *
 * @author Prerna Manaktala
 */
public class Deduper extends AbstractDeduper<Object, Object>
{
  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();
  private Object deduperKey;

  public Object getDeduperKey()
  {
    return deduperKey;
  }

  public void setDeduperKey(Object deduperKey)
  {
    this.deduperKey = deduperKey;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    boolean stateless = context.getValue(Context.OperatorContext.STATELESS);
    if (stateless) {
      bucketManager.setBucketStore(new NonOperationalBucketStore<Object>());
    }
    else {
      ((HdfsBucketStore<Object>)bucketManager.getBucketStore()).setConfiguration(context.getId(), context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
    }
    super.setup(context);
  }

  @Override
  public void bucketLoaded(AbstractBucket<Object> bucket)
  {
    try {
      super.bucketLoaded(bucket);
      eventBucketExchanger.exchange(bucket.bucketKey);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void addEventManuallyToWaiting(Object event)
  {
    waitingEvents.put(bucketManager.getBucketKeyFor(event), Lists.newArrayList(event));
  }

  @Override
  protected Object convert(Object input)
  {
    return input;
  }

  @Override
  protected Object getEventKey(Object event)
  {
    //extract the id from pojo
    throw new UnsupportedOperationException();
    //return id;
  }
}
