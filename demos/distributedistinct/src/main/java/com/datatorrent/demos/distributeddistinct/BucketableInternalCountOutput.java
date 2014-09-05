package com.datatorrent.demos.distributeddistinct;

import java.util.Set;

import com.datatorrent.lib.algo.UniqueValueCount.InternalCountOutput;
import com.datatorrent.lib.bucket.Bucketable;

/**
 * A bucketable version of the InternalCountOutput emitted by UniqueValueCount
 *
 * @param <K> The key for the events that are being counted
 */
class BucketableInternalCountOutput<K> extends InternalCountOutput<K> implements Bucketable
{

  private static final long serialVersionUID = -5490546518097015154L;
  
  private BucketableInternalCountOutput() {
    super();
  }

  public BucketableInternalCountOutput(K k, Integer count, Set<Object> interimUniqueValues)
  {
    super(k, count, interimUniqueValues);
  }

  @Override
  public Object getEventKey()
  {
    return getKey();
  }
}
