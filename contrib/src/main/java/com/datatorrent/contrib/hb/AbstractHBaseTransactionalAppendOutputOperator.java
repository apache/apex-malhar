/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.hb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Append;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;

/**
 * Operator for storing tuples in HBase columns.<br>
 * 
 * <br>
 * This class provides a HBase output operator that can be used to store tuples
 * in columns in a HBase table. It should be extended by the end-operator
 * developer. The extending class should implement operationAppend method and
 * provide a HBase Append metric object that specifies where and what to store
 * for the tuple in the table.<br>
 * 
 * <br>
 * This class provides transactional append where tuples are collected till the
 * end window and they are appended on end window
 * 
 * @param <T> The tuple type
 */
@SuppressWarnings("serial")
public abstract class AbstractHBaseTransactionalAppendOutputOperator<T>
		extends
		AbstractAggregateTransactionableStoreOutputOperator<T, HBaseTransactionalStore> {
	private static final transient Logger logger = LoggerFactory
			.getLogger(AbstractHBaseTransactionalPutOutputOperator.class);
	private transient List<T> tuples;

	public AbstractHBaseTransactionalAppendOutputOperator() {
		store = new HBaseTransactionalStore();
		tuples = new ArrayList<T>();
	}

	@Override
	public void storeAggregate() {

		Iterator<T> it = tuples.iterator();
		while (it.hasNext()) {
			T t = it.next();
			try {
				Append append = operationAppend(t);
				store.getTable().append(append);
			} catch (IOException e) {
				logger.error("Could not append tuple", e);
				DTThrowable.rethrow(e);
			}
			it.remove();
		}
	}

	/**
	 * Return the HBase Append metric to store the tuple. The implementor should
	 * return a HBase Append metric that specifies where and what to store for
	 * the tuple in the table.
	 * 
	 * @param t The tuple
	 * @return The HBase Append metric
	 */
	public abstract Append operationAppend(T t);

	@Override
	public void processTuple(T tuple) {
		tuples.add(tuple);
	}

}
