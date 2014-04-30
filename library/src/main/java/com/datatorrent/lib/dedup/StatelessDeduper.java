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

import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Event;
import com.datatorrent.lib.bucket.StatelessBucketStore;

@Stateless
public abstract class StatelessDeduper<INPUT extends Bucketable & Event, OUTPUT> extends Deduper<INPUT, OUTPUT>
{
  private StatelessBucketStore<INPUT> statelessBucketStore;

  public StatelessDeduper()
  {
    this.statelessBucketStore = new StatelessBucketStore<INPUT>();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    statelessBucketStore.setup();
    super.setup(context);
  }

  @Override
  public void teardown()
  {
    super.teardown();
    statelessBucketStore.teardown();
  }

  public StatelessBucketStore<INPUT> getBucketStore()
  {
    return this.statelessBucketStore;
  }
}
