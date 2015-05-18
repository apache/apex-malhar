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

package com.datatorrent.contrib.aerospike;

import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterObject;

/**
 * <p>
 * A generic implementation of {@link AbstractAerospikeNonTransactionalPutOperator} which can
 * take a POJO.
 * </p>
 * @displayName Aerospike Non-Transactional Put
 * @category Database
 * @tags output operator, put, non-transactional
 * @since 2.1.0
 */
public class AerospikeNonTransactionalPutOperator extends AbstractAerospikeNonTransactionalPutOperator<Object>
{
  private static transient final Logger LOG
    = LoggerFactory.getLogger(AerospikeNonTransactionalPutOperator.class);

  // Two element list; first retrieves the record key and second the list of bins in this tuple
  @NotNull
  private ArrayList<String> expressions;

  private GetterObject keyGetter;
  private GetterObject binsGetter;

  // required by App Builder
  public AerospikeNonTransactionalPutOperator()
  {
  }

  /*
   * Two Java expressions that will yield the key and the list of modified Bins
   * for the destination record of this tuple
   * Example: {"getKey()", "getBins()"}
   */
  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  /*
   * Set field retrieval list of expressions.
   * @param ArrayList of field retrieval expressions
   */
  public void setExpressions(ArrayList<String> e)
  {
    this.expressions = e;
  }

  /*
   * Not a property, so don't show in App Builder
   * @omitFromUI
   */
  @Override
  public Key getUpdatedBins(Object tuple, List<Bin> list)
  {
    if (null == keyGetter) {    // first tuple
      Class<?> tupleClass = tuple.getClass();
      keyGetter  = PojoUtils.createGetterObject(tupleClass, expressions.get(0));
      binsGetter = PojoUtils.createGetterObject(tupleClass, expressions.get(1));
    }
    Key key = (Key)keyGetter.get(tuple);
    List<Bin> binList = (List<Bin>)binsGetter.get(tuple);
    if ( ! (null == binList || binList.isEmpty()) ) {
      list.addAll(binList);
    }
    return key;
  }

}
