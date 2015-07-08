/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

import com.couchbase.client.protocol.views.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.db.AbstractStoreInputOperator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * <p>
 * CouchBasePOJOInputOperator</p>
 * A generic implementation of AbstractStoreInputOperator that fetches rows of data from Couchbase store and emits them as POJOs.
 * Each row is converted to a POJO.User needs to specify the design document name and view name against which he wants to query.
 * User should also provide a mapping function to fetch the specific fields from database.The query is generated using the mapping
 * function on top of the view.User has the option to specify the start key and limit of number of documents he wants to view.
 * He can also specify whether he wants to view results in descending order or not.Also he can specify keys to filter results based on those keys.
 * The start value is continuously updated with the value of the key of the last row from the result of the previous run of the query and skip parameter is set to 1.
 * The skip parameter is reset if there are empty tuples.
 * Example:
 * function (doc) {
 * emit(doc._id, [doc.username, doc.first_name, doc.last_name, doc.last_login]);
 * }
 *
 * @displayName Couchbase POJO Input Operator
 * @category Input
 * @tags input operator
 */
public class CouchBasePOJOInputOperator extends AbstractStoreInputOperator<Object, CouchBaseStore>
{
  private transient Class<?> className = null;
  private transient Query query;
  private final transient ObjectMapper objectMapper;
  //User has the option to specify a start key.
  private String startkey;
  @Min(1)
  private int limit = 10;
  private String startDocId;
  @NotNull
  private String designDocumentName;
  @NotNull
  private String viewName;
  private int skip = 0;
  //Keys for filtering results from viewquery.
  private ArrayList<String> keys;
  /*
   * POJO class which is generated as output from this operator.
   * Example:
   * public class TestPOJO{ int intfield; public int getInt(){} public void setInt(){} }
   * outputClass = TestPOJO
   * POJOs will be generated on fly in later implementation.
   */
  private String outputClass;
  //User gets the option to specify the order of documents.
  private boolean descending;

  public boolean isDescending()
  {
    return descending;
  }

  public void setDescending(boolean descending)
  {
    this.descending = descending;
  }

  public String getOutputClass()
  {
    return outputClass;
  }

  public void setOutputClass(String outputClass)
  {
    this.outputClass = outputClass;
  }

  public String getStartDocId()
  {
    return startDocId;
  }

  public void setStartDocId(String startDocId)
  {
    this.startDocId = startDocId;
  }

  public ArrayList<String> getKeys()
  {
    return keys;
  }

  public void setKeys(ArrayList<String> keys)
  {
    this.keys = keys;
  }


  /*
   * Name of the design document in which the view to be queried is added.
   */
  public String getDesignDocumentName()
  {
    return designDocumentName;
  }

  public void setDesignDocumentName(String designDocumentName)
  {
    this.designDocumentName = designDocumentName;
  }

  /*
   * Name of the view against which a user wants to query.
   */
  public String getViewName()
  {
    return viewName;
  }

  public void setViewName(String viewName)
  {
    this.viewName = viewName;
  }

  public int getLimit()
  {
    return limit;
  }

  public void setLimit(int limit)
  {
    this.limit = limit;
  }

  public String getStartkey()
  {
    return startkey;
  }

  public void setStartkey(String startkey)
  {
    this.startkey = startkey;
  }

  public CouchBasePOJOInputOperator()
  {
    objectMapper = new ObjectMapper();
    store = new CouchBaseStore();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    try {
      className = Class.forName(outputClass);
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
    query = new Query();
    query.setStale(Stale.FALSE);
    query.setIncludeDocs(true);
    query.setLimit(limit);
    query.setDescending(descending);
    if (keys != null) {
      for (int i = 0; i < keys.size(); i++) {
        query.setKey(keys.get(i));
      }
    }
  }

  @Override
  public void emitTuples()
  {
    if (startkey != null) {
      query.setRangeStart(startkey);
    }
    if (skip == 1) {
      query.setSkip(skip);
    }
    View view = store.getInstance().getView(designDocumentName, viewName);

    ViewResponse result = store.getInstance().query(view, query);

    Iterator<ViewRow> iterRow = result.iterator();
    if (!iterRow.hasNext()) {
      skip = 0;
    }
    else {
      while (iterRow.hasNext()) {
        ViewRow row = iterRow.next();
        Object document = row.getDocument();
        Object outputObj = null;
        try {
          outputObj = objectMapper.readValue(document.toString(), className);
        }
        catch (IOException ex) {
          throw new RuntimeException(ex);
        }
        //Update start key if it is specified.
        if (startkey != null) {
          startkey = row.getKey();
          skip = 1;
        }
        outputPort.emit(outputObj);

      }
    }

  }

}
