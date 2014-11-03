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

import java.io.IOException;
import java.util.List;

import com.datatorrent.lib.db.AbstractStoreInputOperator;

import com.datatorrent.api.Context;

import com.datatorrent.common.util.DTThrowable;

/**
 * AbstractCouchBaseInputOperator which extends AbstractStoreInputOperator.
 * Classes extending from this operator should implement the abstract functionality of getTuple and getKeys.
 */
public abstract class AbstractCouchBaseInputOperator<T> extends AbstractStoreInputOperator<T, CouchBaseStore>
{

  public AbstractCouchBaseInputOperator()
  {
    store = new CouchBaseStore();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void emitTuples()
  {
    List<String> keys = getKeys();
    for (String key : keys) {
      try {
        Object result = store.getInstance().get(key);
        T tuple = getTuple(result);
        outputPort.emit(tuple);
      }
      catch (Exception ex) {
        try {
          store.disconnect();
        }
        catch (IOException ex1) {
          DTThrowable.rethrow(ex1);
        }
        DTThrowable.rethrow(ex);
      }
    }

  }

  public abstract T getTuple(Object object);

  public abstract List<String> getKeys();

}
