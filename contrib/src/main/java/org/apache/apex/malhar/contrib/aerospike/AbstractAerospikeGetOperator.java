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
package org.apache.apex.malhar.contrib.aerospike;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.AbstractStoreInputOperator;

import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Base input adapter, which reads data from persistence database through its
 * API and writes into output port(s). Subclasses should provide the
 * implementation of getting the tuples and querying to retrieve data.
 * <p>
 * This is an abstract class. Sub-classes need to implement
 * {@link #queryToRetrieveData()} and {@link #getTuple(Record)}.
 *
 * @displayName Abstract Aerospike Get
 * @category Input
 * @tags get
 * @since 1.0.4
 */
public abstract class AbstractAerospikeGetOperator<T> extends AbstractStoreInputOperator<T, AerospikeStore>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractAerospikeGetOperator.class);

  /**
   * Any concrete class has to override this method to convert a Database row into Tuple.
   *
   * @param row a single row that has been read from database.
   * @return Tuple a tuples created from row which can be any Java object.
   */
  public abstract T getTuple(Record record);

  /**
   * Any concrete class has to override this method to return the query string which will be used to
   * retrieve data from database.
   *
   * @return Query string
   */
  public abstract Statement queryToRetrieveData();

  /**
   * The output port that will emit tuple into DAG.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * This executes the query to retrieve result from database.
   * It then converts each row into tuple and emit that into output port.
   */
  @Override
  public void emitTuples()
  {
    Statement query = queryToRetrieveData();
    logger.debug(String.format("select statement: %s", query.toString()));
    RecordSet rs;
    try {
      rs = store.getClient().query(null, query);
      while (rs.next()) {
        Record rec = rs.getRecord();
        T tuple = getTuple(rec);
        outputPort.emit(tuple);
      }
    } catch (Exception ex) {
      store.disconnect();
      DTThrowable.rethrow(ex);
    }
  }
}
