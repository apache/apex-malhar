/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.dedup;



import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.bucket.*;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerImpl;

/*
 *
 * @displayName CustomDeduper
 * @category Deduplication
 * @tags dedupe
 *
 * @param <INPUT>  type of input tuple
 * @param <OUTPUT> type of output tuple
 *

public abstract class CustomDeduper<INPUT extends BucketableCustomKey & Event, OUTPUT>
  extends DeduperWithHdfsStore<INPUT , OUTPUT>
{

  private TimeBasedBucketManagerImpl timespan;
  private int daysSpan;
  @Min(1)
  protected long bucketSpanInMillis;


  public CustomDeduper()
  {
    timespan = new TimeBasedBucketManagerImpl();
  }

  public int getDaysSpan()
  {
    daysSpan = timespan.getDaysSpan();
    return daysSpan;
  }

  public void setDaysSpan(int daysSpan)
  {
    timespan.setDaysSpan(daysSpan);
  }

  public long getBucketSpanInMillis()
  {
    bucketSpanInMillis = timespan.getBucketSpanInMillis();
    return bucketSpanInMillis;
  }

  public void setBucketSpanInMillis(long bucketSpanInMillis)
  {
    timespan.setBucketSpanInMillis(bucketSpanInMillis);
  }

  private final static Logger logger = LoggerFactory.getLogger(CustomDeduper.class);

}*/
