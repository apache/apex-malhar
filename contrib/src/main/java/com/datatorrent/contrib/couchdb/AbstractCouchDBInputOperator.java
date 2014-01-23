/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.couchdb;

import java.io.IOException;

import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;

import com.google.common.base.Throwables;

import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.lib.db.AbstractStoreInputOperator;

/**
 * Base class for CouchDb intput adaptor.<br/>
 * <p>
 * CouchDb filters documents in the database using stored views. Views are refered as design documents.
 * This operator queries the view and emits the view result.
 * </p>
 *
 * <p>
 * Subclasses  of this operator provide the ViewQuery which corresponds to a database view.</br>
 * In this base implementaion, if the ViewQuery doesn't change, then the same view results are emitted
 * at the end of every streaming window.
 * </p>
 *
 * @param <T>Type of tuples which are generated</T>
 * @since 0.3.5
 */
@ShipContainingJars(classes = {ViewQuery.class})
public abstract class AbstractCouchDBInputOperator<T> extends AbstractStoreInputOperator<T, CouchDbStore>
{
  @Override
  public void emitTuples()
  {
    ViewQuery viewQuery = getViewQuery();
    ViewResult result = store.queryStore(viewQuery);
    try {
      for (ViewResult.Row row : result.getRows()) {
        T tuple = getTuple(row);
        outputPort.emit(tuple);
      }
    }
    catch (Throwable cause) {
      Throwables.propagate(cause);
    }
  }

  /**
   * @return view-query that specifies the couch-db view whose results will be fetched.
   */
  public abstract ViewQuery getViewQuery();

  /**
   * This operator fetches result of a view in {@link ViewResult}. Sub-classes should provie the
   * implementaion to convert a row of ViewResult to emitted tuple type.
   *
   * @param value a row of ViewResult that should be converted to a tuple.
   * @return emmitted tuple.
   */
  public abstract T getTuple(ViewResult.Row value) throws IOException;
}
