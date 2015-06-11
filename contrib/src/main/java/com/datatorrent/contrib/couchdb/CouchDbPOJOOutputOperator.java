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

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;

/**
 * Implementation of {@link AbstractCouchDBOutputOperator} that saves a POJO in the couch database. <br/>
 * <p>
 * @displayName POJO Based CouchDb Output Operator
 * @category Database
 * @tags output operator
 * @since 0.3.5
 */
public class CouchDbPOJOOutputOperator extends AbstractCouchDBOutputOperator<Object>
{
  private transient Getter<Object, String> getterDocId;
  private transient Getter<Object, ? extends Object> valueGetter;
  @NotNull
  private ArrayList<String> expressions;
  @NotNull
  private ArrayList<String> classNamesOfFields;
  @NotNull
  private String expressionForDocId;
  @NotNull
  private String expressionForDocRevision;

  public String getExpressionForDocRevision()
  {
    return expressionForDocRevision;
  }

  public void setExpressionForDocRevision(String expressionForDocRevision)
  {
    this.expressionForDocRevision = expressionForDocRevision;
  }

  /*
   * An ArrayList of classnames for fields inside pojo.
   */
  public ArrayList<String> getClassNamesOfFields()
  {
    return classNamesOfFields;
  }

  public void setClassNamesOfFields(ArrayList<String> classNamesOfFields)
  {
    this.classNamesOfFields = classNamesOfFields;
  }

  /*
   * An ArrayList of Java expressions that will yield the document field values from the POJO.
   */
  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(ArrayList<String> expressions)
  {
    this.expressions = expressions;
  }

  /*
   * An Expression to extract value of document Id from input POJO.
   */
  public String getExpressionForDocId()
  {
    return expressionForDocId;
  }

  public void setExpressionForDocId(String expressionForDocId)
  {
    this.expressionForDocId = expressionForDocId;
  }

  @Override
  public String getDocumentId(Object tuple)
  {
    if (getterDocId == null) {
      getterDocId = PojoUtils.createGetter(tuple.getClass(), expressionForDocId, String.class);
    }
    String docId = getterDocId.get(tuple);
    return docId;
  }

  @Override
  public Map<?,?> convertTupleToMap(Object tuple)
  {
    Map<Object, Object> mapTuple = new HashMap<Object, Object>();
    mapTuple.put("_id", getDocumentId(tuple));
    if (valueGetter == null) {
      int size = expressions.size();
      for (int i = 0; i < size; i++) {
        try {
          valueGetter = PojoUtils.createGetter(tuple.getClass(), expressions.get(i), Class.forName("java.lang." + classNamesOfFields.get(i)));
        }
        catch (ClassNotFoundException ex) {
          throw new RuntimeException(ex);
        }
        mapTuple.put(expressions.get(i), valueGetter.get(tuple));
      }
    }
    return mapTuple;
  }

}
