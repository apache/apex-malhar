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
package com.datatorrent.contrib.couchbase;

import com.datatorrent.api.Context;

import net.spy.memcached.internal.OperationFuture;

/**
 * AbstractUpdateCouchBaseOutputOperator which extends AbstractCouchBaseOutputOperator and implements add functionality of couchbase.
 */
public abstract class AbstractUpdateCouchBaseOutputOperator<T> extends AbstractCouchBaseOutputOperator<T>
{

  private transient CompletionListener listener;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    listener = new CompletionListener();
  }

  @Override
  public void processKeyValue(String key, Object value)
  {
    OperationFuture<Boolean> future = store.getInstance().add(key, value);
    future.addListener(listener);
  }

}
