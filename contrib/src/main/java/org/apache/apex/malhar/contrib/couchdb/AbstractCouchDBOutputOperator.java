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

import org.apache.apex.malhar.lib.db.AbstractStoreOutputOperator;

/**
 * Generic base output adaptor which saves tuples in the CouchDb.&nbsp; Subclasses should provide implementation for getting Document Id. <br/>
 * <p>
 * An {@link AbstractStoreOutputOperator} saving tuples in the CouchDb.
 * Sub-classes provide the implementation of parsing document id from the tuple and converting tuple to a map.
 * @displayName Abstract CouchDB Output
 * @category Output
 * @tags output operator
 * @param <T> type of tuple </T>
 * @since 0.3.5
 */
public abstract class AbstractCouchDBOutputOperator<T> extends AbstractStoreOutputOperator<T, CouchDbStore>
{

  AbstractCouchDBOutputOperator()
  {
    super();
  }

  @Override
  public void processTuple(T tuple)
  {
    store.upsertDocument(getDocumentId(tuple), tuple);
  }

  /**
   * Get document id from the tuple.
   *
   * @param tuple tuple to be saved.
   * @return document id.
   */
  public abstract String getDocumentId(T tuple);

}
