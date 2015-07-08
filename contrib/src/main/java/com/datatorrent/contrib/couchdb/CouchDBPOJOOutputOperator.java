/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchdb;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * Implementation of {@link AbstractCouchDBOutputOperator} that saves a POJO in the couch database. <br/>
 * <p>
 * @displayName POJO Based CouchDb Output Operator
 * @category Database
 * @tags output operator
 * @since 0.3.5
 */
public class CouchDBPOJOOutputOperator extends AbstractCouchDBOutputOperator<Object>
{
  private static final long serialVersionUID = 201506181121L;
  private transient Getter<Object, String> getterDocId;

  @NotNull
  private String expressionForDocId;

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

}
