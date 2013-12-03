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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.map.ObjectMapper;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;

import javax.annotation.Nonnull;

/**
 * <br>Base class for CouchDb intput adaptor.</br>
 * <br>CouchDb filters documents in the database using stored views. Views are refered as design documents.
 * This operator queries the view and emits the view result.</br>
 * <p/>
 * <br>Subclasses  of this operator provide the ViewQuery which corresponds to a database view.</br>
 * <br>In this base implementaion, if the ViewQuery doesn't change, then the same view results are emitted
 * at the end of every streaming window.</br>
 *
 * @param <T>Type of tuples which are generated</T>
 * @since 0.3.5
 */
@ShipContainingJars(classes = {ObjectMapper.class, ViewQuery.class, ViewResult.class})
public abstract class AbstractCouchDBInputOperator<T> extends BaseOperator implements CouchDbOperator, InputOperator
{

  private String url;
  @Nonnull
  private String dbName;
  private String userName;
  private String password;

  protected transient CouchDBLink dbLink;
  protected transient ObjectMapper mapper;

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  @Override
  public void emitTuples()
  {
    ViewQuery viewQuery = getViewQuery();
    ViewResult result = dbLink.getConnector().queryView(viewQuery);
    for (ViewResult.Row row : result.getRows()) {
      T tuple = getTuple(row);
      outputPort.emit(tuple);
    }
  }

  @Override
  public void setUrl(String url)
  {
    this.url = url;
  }

  @Override
  public void setDatabase(String dbName)
  {
    this.dbName = Preconditions.checkNotNull(dbName, "database");
  }

  @Override
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  @Override
  public void setPassword(String password)
  {
    this.password = password;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.dbLink = new CouchDBLink(url, userName, password, dbName);
    this.mapper = new ObjectMapper();
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
  public abstract T getTuple(ViewResult.Row value);
}
