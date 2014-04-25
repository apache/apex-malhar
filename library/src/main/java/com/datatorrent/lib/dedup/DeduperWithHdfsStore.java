/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.dedup;

import java.util.Map;

import javax.annotation.Nonnull;

import com.datatorrent.api.Context;

import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.HdfsBucketStore;

/**
 * {@link Deduper} that uses hdfs to store buckets.
 */
public abstract class DeduperWithHdfsStore<INPUT extends Bucketable, OUTPUT> extends Deduper<INPUT, OUTPUT>
{
  @Nonnull
  private String bucketsDir;
  @Nonnull
  private HdfsBucketStore<INPUT> bucketStore;

  public DeduperWithHdfsStore()
  {
    super();
    bucketsDir = "buckets";
  }

  public void setBucketsDir(@Nonnull String bucketsDir)
  {
    this.bucketsDir = bucketsDir;
  }

  @Override
  public void partitioned(Map<Integer, Partition<Deduper<INPUT, OUTPUT>>> partitions)
  {
    super.partitioned(partitions);
    for (Partition<Deduper<INPUT, OUTPUT>> partition : partitions.values()) {
      DeduperWithHdfsStore<INPUT, OUTPUT> deduperWithHdfsStore = (DeduperWithHdfsStore<INPUT, OUTPUT>) partition.getPartitionedInstance();
      deduperWithHdfsStore.bucketsDir = this.bucketsDir;
      deduperWithHdfsStore.bucketStore = this.bucketStore;
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    bucketStore.setConfiguration(context.getId(), bucketsDir, partitionKeys, partitionMask);
    bucketStore.setup();
    super.setup(context);
  }

  @Override
  public void teardown()
  {
    super.teardown();
    bucketStore.teardown();
  }

  public void setBucketStore(@Nonnull HdfsBucketStore<INPUT> bucketStore)
  {
    this.bucketStore = bucketStore;
  }

  public HdfsBucketStore<INPUT> getBucketStore()
  {
    return this.bucketStore;
  }
}
