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
import java.util.List;

import javax.validation.constraints.Min;

import com.google.common.base.Throwables;

import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;

import com.datatorrent.lib.db.AbstractStoreInputOperator;

import com.datatorrent.api.annotation.ShipContainingJars;

/**
 * Base class for CouchDb input adaptor.<br/>
 * <p>
 * CouchDb filters documents in the database using stored views. Views are referred as design documents.
 * This operator queries the view and emits the view result.
 * </p>
 *
 * <p>
 * Subclasses of this operator provide the ViewQuery which corresponds to a database view.<br/>
 * In this base implementation, if the ViewQuery doesn't change, then the same view results are emitted
 * at the end of every streaming window.
 * </p>
 *
 * <p>
 * The operator can emit paged results as well. The page size is configured using {@link #pageSize}.<br/>
 * The operator assumes that the ViewQuery implementation by sub-classes does not depend on data outside the document
 * (like the current date) because that will break the caching of a view's result in CouchDb.
 * Also the {@link #getViewQuery()} method should return the same view stored in CouchDb every time.<br/>
 * </p>
 *
 * @param <T> Type of tuples which are generated</T>
 * @since 0.3.5
 */
@ShipContainingJars(classes = {ViewQuery.class})
public abstract class AbstractCouchDBInputOperator<T> extends AbstractStoreInputOperator<T, CouchDbStore>
{
  @Min(0)
  private int pageSize;
  private String nextPageKey = null;
  private boolean started = false;

  @Override
  public void emitTuples()
  {
    if (pageSize == 0) {
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
    else {
      if (!started || nextPageKey != null) {
        started = true;
        ViewQuery query = getViewQuery().limit(pageSize + 1);

        if (nextPageKey != null) {
          query.startKey(nextPageKey);
        }
        ViewResult result = store.queryStore(query);
        List<ViewResult.Row> rows = result.getRows();
        List<ViewResult.Row> rowsToEmit = rows;
        if (rows.size() > pageSize) {
          //More pages to fetch. We don't emit the last row as it is a link to next page.
          nextPageKey = rows.get(rows.size() - 1).getKey();
          rowsToEmit = rows.subList(0, rows.size() - 1);
        }
        else {
          //No next page so emit all the rows.
          nextPageKey = null;
        }
        try {
          for (ViewResult.Row row : rowsToEmit) {
            T tuple = getTuple(row);
            outputPort.emit(tuple);
          }
        }
        catch (Throwable cause) {
          Throwables.propagate(cause);
        }
      }
    }
  }

  /**
   * @return view-query that specifies the couch-db view whose results will be fetched.
   */
  public abstract ViewQuery getViewQuery();

  /**
   * This operator fetches result of a view in {@link ViewResult}. Sub-classes should provide the
   * implementation to convert a row of ViewResult to emitted tuple type.
   *
   * @param value a row of ViewResult that should be converted to a tuple.
   * @return emitted tuple.
   * @throws IOException 
   */
  public abstract T getTuple(ViewResult.Row value) throws IOException;

  /**
   * Sets the no. of rows in a page.
   *
   * @param pageSize size of a page
   */
  public void setPageSize(int pageSize)
  {
    this.pageSize = pageSize;
  }

}
