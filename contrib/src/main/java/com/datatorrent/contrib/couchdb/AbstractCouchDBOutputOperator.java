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

import com.datatorrent.lib.db.AbstractStoreOutputOperator;
import java.util.Map;

/**
 * Generic base output adaptor which saves tuples in the CouchDb.&nbsp; Subclasses should provide implementation for getting Document Id. <br/>
 * <p>
 * An {@link AbstractStoreOutputOperator} saving tuples in the CouchDb.
 * Sub-classes provide the implementation of parsing document id from the tuple and converting tuple to a map.
 * @displayName Abstract CouchDB Output
 * @category Database
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
    store.upsertDocument(getDocumentId(tuple), convertTupleToMap(tuple));
  }

  /**
   * Get document id from the tuple.
   *
   * @param tuple tuple to be saved.
   * @return document id.
   */
  public abstract String getDocumentId(T tuple);

  /*
   * Converts Tuple to Map.
   */
  public abstract Map<?,?> convertTupleToMap(T tuple);

}
