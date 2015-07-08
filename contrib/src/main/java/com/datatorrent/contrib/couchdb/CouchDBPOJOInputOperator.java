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
package com.datatorrent.contrib.couchdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult.Row;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.*;

import com.datatorrent.api.Context.OperatorContext;

/**
 * <p>
 * CouchDbPOJOInputOperator</p>
 * A generic implementation of AbstractCouchDBInputOperator that fetches ViewResult rows from CouchDb and emits them as POJOs.
 * Each row is read as a jsonNode and converted into POJO using an ObjectMapper.
 * The POJO generated can contain the document Id of the document fetched.
 * User needs to specify the design document name and view name against which he wants to query.
 * User should also provide a mapping function to fetch the specific fields from database.The View query is generated using the mapping
 * function on top of the view.User has the option to specify the start key and limit of number of documents he wants to view.
 * He can also specify whether he wants to view results in descending order or not.
 * This implementation uses the emitTuples implementation of {@link AbstractCouchDBInputOperator} to emits the results of the ViewQuery.
 * Example of mapping function:
 * function (doc) {
 * emit(doc._id, doc);
 * }
 *
 */
public class CouchDBPOJOInputOperator extends AbstractCouchDBInputOperator<Object>
{
  //List of expressions set by User. Example:setId(),setName(),Address
  @NotNull
  private List<String> expressions;
  private String expressionForDocId;
  // List of columns provided by User. Example: id,name,address
  @NotNull
  private List<String> columns;
  @NotNull
  private String designDocumentName;
  @NotNull
  private String viewName;
  private transient ViewQuery query;
  private transient Setter<Object, String> setterDocId;
  private transient List<Object> setterDoc;
  //User gets the option to specify the order of documents.
  private boolean descending;

  private final transient ObjectMapper mapper;
  /*
   * POJO class which is generated as output from this operator.
   * Example:
   * public class TestPOJO{ int intfield; public int getInt(){} public void setInt(){} }
   * outputClass = TestPOJO
   * POJOs will be generated on fly in later implementation.
   */
  private transient Class<?> objectClass = null;
  private String outputClass;

  private final transient List<Class<?>> fieldType;

  public CouchDBPOJOInputOperator()
  {
    mapper = new ObjectMapper();
    fieldType = new ArrayList<Class<?>>();
    this.store = new CouchDbStore();
  }

  /*
   * List of Expressions to extract value of fields from couch db and set in the POJO.
   */
  public List<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(List<String> expressions)
  {
    this.expressions = expressions;
  }

  public String getDesignDocumentName()
  {
    return designDocumentName;
  }

  public void setDesignDocumentName(String designDocumentName)
  {
    this.designDocumentName = designDocumentName;
  }

  public String getViewName()
  {
    return viewName;
  }

  public void setViewName(String viewName)
  {
    this.viewName = viewName;
  }

  public boolean isDescending()
  {
    return descending;
  }

  public void setDescending(boolean descending)
  {
    this.descending = descending;
  }

  /*
   * List of columns which specify field names to be set in POJO.
   */
  public List<String> getColumns()
  {
    return columns;
  }

  public void setColumns(List<String> columns)
  {
    this.columns = columns;
  }

  /*
   * An Expression to extract value of document Id from couch db and set in the POJO.
   */
  public String getExpressionForDocId()
  {
    return expressionForDocId;
  }

  public void setExpressionForDocId(String expressionForDocId)
  {
    this.expressionForDocId = expressionForDocId;
  }

  public String getOutputClass()
  {
    return outputClass;
  }

  public void setOutputClass(String outputClass)
  {
    this.outputClass = outputClass;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    setterDoc = new ArrayList<Object>();
    query = new ViewQuery().designDocId(designDocumentName).viewName(viewName).descending(descending);

    try {
      // This code will be replaced after integration of creating POJOs on the fly utility.
      objectClass = Class.forName(outputClass);
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }

    if (expressionForDocId != null) {
      setterDocId = PojoUtils.createSetter(objectClass, expressionForDocId, String.class);
    }

    for (int i = 0; i < expressions.size(); i++) {
      Class<?> type = null;
      try {
        type = objectClass.getDeclaredField(columns.get(i)).getType();
      }
      catch (NoSuchFieldException ex) {
        throw new RuntimeException(ex);
      }
      catch (SecurityException ex) {
        throw new RuntimeException(ex);
      }
      fieldType.add(type);
      if (type.isPrimitive()) {
        setterDoc.add(PojoUtils.constructSetter(objectClass, expressions.get(i), type));
      }
      else {
        setterDoc.add(PojoUtils.createSetter(objectClass, expressions.get(i), type));
      }
    }

  }

  @Override
  @SuppressWarnings("unchecked")
  public Object getTuple(Row value) throws IOException
  {
    Object obj;
    try {
      obj = objectClass.newInstance();
    }
    catch (InstantiationException ex) {
      throw new RuntimeException(ex);
    }
    catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }

    if (setterDocId != null) {
      setterDocId.set(obj, value.getId());
    }

    JsonNode val = value.getValueAsNode();
    for (int i = 0; i < setterDoc.size(); i++) {
      Class<?> type = fieldType.get(i);
      if (type.isPrimitive()) {
        if (type == int.class) {
          ((SetterInt)setterDoc.get(i)).set(obj, val.get(columns.get(i)).getIntValue());
        }
        else if (type == boolean.class) {
          ((SetterBoolean)setterDoc.get(i)).set(obj, val.get(columns.get(i)).getBooleanValue());
        }
        else if (type == long.class) {
          ((SetterLong)setterDoc.get(i)).set(obj, val.get(columns.get(i)).getLongValue());
        }
        else if (type == double.class) {
          ((SetterDouble)setterDoc.get(i)).set(obj, val.get(columns.get(i)).getDoubleValue());
        }
        else {
          throw new RuntimeException("Type is not supported");
        }
      }
      else {
        ((Setter<Object, Object>)setterDoc.get(i)).set(obj, mapper.readValue(val.get(columns.get(i)), type));
      }
    }
    return obj;
  }

  @Override
  public ViewQuery getViewQuery()
  {
    /*
    // The skip option should only be used with small values, as skipping a large range of documents this way is inefficient.
    if (skip == 1) {
      query.skip(skip);
    }
    */
    return query;
  }

}
