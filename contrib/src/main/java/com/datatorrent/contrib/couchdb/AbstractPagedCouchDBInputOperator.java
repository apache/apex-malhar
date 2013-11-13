/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.couchdb;

import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;

import javax.validation.constraints.Min;
import java.util.List;

/**
 * <br>This operator emits paged results. The page size is configured using operator property.</br>
 * <br>The operator assumes that the ViewQuery implementation by sub-classes does not depend on data oustide the document
 * (like the current date) because that will break the caching of a view's result in CouchDb.
 * Also the getViewQuery() method should return the same view stored in CouchDb everytime. </br>
 *
 * @since 0.3.5
 */
public abstract class AbstractPagedCouchDBInputOperator<T> extends AbstractCouchDBInputOperator<T>
{
  @Min(0)
  private int pageSize;

  private String nextPageKey = null;
  private boolean started = false;

  public AbstractPagedCouchDBInputOperator()
  {
    super();
    pageSize = 0;
  }

  /**
   * Sets the no. of rows in a page.
   *
   * @param pageSize size of a page
   */
  public void setPageSize(int pageSize)
  {
    this.pageSize = pageSize;
  }

  @Override
  public void emitTuples()
  {
    if (pageSize == 0)     //No pagination
      super.emitTuples();
    else {
      if (!started || nextPageKey != null) {
        started = true;
        ViewQuery query = getViewQuery().limit(pageSize + 1);

        if (nextPageKey != null)
          query.startKey(nextPageKey);
        ViewResult result = dbLink.getConnector().queryView(query);
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
        for (ViewResult.Row row : rowsToEmit) {
          T tuple = getTuple(row);
          outputPort.emit(tuple);
        }
      }
    }
  }
}
