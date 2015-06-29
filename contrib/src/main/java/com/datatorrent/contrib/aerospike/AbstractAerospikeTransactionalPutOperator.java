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
package com.datatorrent.contrib.aerospike;

import java.util.Collection;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.google.common.collect.Lists;

import com.datatorrent.lib.db.AbstractBatchTransactionableStoreOutputOperator;

/**
 * <p>
 * Generic base adaptor which creates a transaction at the start of window.&nbsp; Subclasses should provide implementation for getting updated bins. <br/>
 * </p>
 * <p>
 * Executes all the put updates and closes the transaction at the end of the window.
 * The tuples in a window are stored in check-pointed collection which is cleared in the endWindow().
 * This is needed for the recovery. The operator writes a tuple at least once in the database, which is why
 * only when all the updates are executed, the transaction is committed in the end window call.
 * </p>
 * @displayName Abstract Aerospike Transactional Put
 * @category Database
 * @tags output operator, put, transactional
 * @param <T>type of tuple
 * @since 1.0.4
 */
public abstract class AbstractAerospikeTransactionalPutOperator<T> extends AbstractBatchTransactionableStoreOutputOperator<T, AerospikeTransactionalStore> {

  private transient final List<Bin> bins;

  public AbstractAerospikeTransactionalPutOperator() {
    super();
    bins = Lists.newArrayList();
  }

  /**
   * Any concrete class needs to implement this method which using the input tuple, adds the
   * modified bins to bins list and returns the key for that updated record.
   * 
   * @param tuple Tuple coming in from input port
   * @param bins list of bins that would be updated for this tuple
   * @return key for the row to be updated in the database
   * @throws AerospikeException
   */
  protected abstract Key getUpdatedBins(T tuple, List<Bin> bins) throws AerospikeException;

  @Override
  public void processBatch(Collection<T> tuples)
  {
    Key key;
    Bin[] binsArray;
    try {
      for(T tuple: tuples) {
        key = getUpdatedBins(tuple,bins);
        binsArray = new Bin[bins.size()];
        binsArray = bins.toArray(binsArray);
        store.getClient().put(null, key, binsArray);
        bins.clear();
      }
    } catch (AerospikeException e) {
      throw new RuntimeException(e);
    }
  }
}
