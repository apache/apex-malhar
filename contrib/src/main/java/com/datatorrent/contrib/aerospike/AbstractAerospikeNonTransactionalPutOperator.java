/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.contrib.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.datatorrent.lib.db.AbstractStoreOutputOperator;
import org.python.google.common.collect.Lists;

import java.util.List;


/**
 * <p>
 * Generic Output Adaptor which writes tuples as they come without providing any transactional support.
 * </p>
 * @displayName: Abstract Aerospike Non Transactional Put Operator
 * @category: db
 * @tag: output operator, put, non transactional
 * @param <T>type of tuple</T>
 * @since 1.0.4
 */
public abstract class AbstractAerospikeNonTransactionalPutOperator<T> extends AbstractStoreOutputOperator<T,AerospikeStore> {

  private transient List<Bin> bins;
  public AbstractAerospikeNonTransactionalPutOperator() {
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
  protected abstract Key getUpdatedBins(T tuple,List<Bin> bins) throws AerospikeException;

  @Override
  public void processTuple(T tuple) {

    Key key=null;
    Bin[] binsArray = null;
    try {
      key = getUpdatedBins(tuple,bins);
      binsArray=new Bin[bins.size()];
      binsArray=bins.toArray(binsArray);
      store.getClient().put(null, key, binsArray);
      bins.clear();
    }
    catch (AerospikeException e) {
      throw new RuntimeException(e);
    }

  }

}
