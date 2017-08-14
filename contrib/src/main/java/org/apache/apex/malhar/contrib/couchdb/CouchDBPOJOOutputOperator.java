/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.couchdb;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Implementation of {@link AbstractCouchDBOutputOperator} that saves a POJO in the couch database. <br/>
 * <p>
 * @displayName CouchDb Output Operator
 * @category Output
 * @tags database, nosql, pojo, couchdb
 * @since 0.3.5
 */
@Evolving
public class CouchDBPOJOOutputOperator extends AbstractCouchDBOutputOperator<Object>
{
  private static final long serialVersionUID = 201506181121L;
  private transient Getter<Object, String> getterDocId;

  @NotNull
  private String expressionForDocId;

  /**
   * Gets the getter expression for the document Id.
   * @return The document Id.
   */
  public String getExpressionForDocId()
  {
    return expressionForDocId;
  }

  /**
   * An Expression to extract value of document Id from an input POJO.
   * @param expressionForDocId The getter expression for the document Id.
   */
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
