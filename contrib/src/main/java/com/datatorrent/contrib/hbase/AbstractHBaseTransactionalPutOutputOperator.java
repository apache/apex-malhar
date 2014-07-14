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
package com.datatorrent.contrib.hbase;

import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Operator for storing tuples in HBase rows.<br>
 * 
 * <br>
 * This class provides a HBase output operator that can be used to store tuples
 * in rows in a HBase table. It should be extended by the end-operator
 * developer. The extending class should implement operationPut method and
 * provide a HBase Put metric object that specifies where and what to store for
 * the tuple in the table.<br>
 * 
 * <br>
 * This class provides a batch put where tuples are collected till the end
 * window and they are put on end window
 * 
 * Note that since HBase doesn't support transactions this store cannot
 * guarantee each tuple is written only once to HBase in case the operator is
 * restarted from an earlier checkpoint. It only tries to minimize the number of
 * duplicates limiting it to the tuples that were processed in the window when
 * the operator shutdown.
 * 
 * @param <T>
 *            The tuple type
 * @since 1.0.2
 */
@ShipContainingJars(classes = { org.apache.hadoop.hbase.client.HTable.class,
		org.apache.hadoop.hbase.util.BloomFilterFactory.class,
		com.google.protobuf.AbstractMessageLite.class,
		org.apache.hadoop.hbase.BaseConfigurable.class,
		org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.class,
		org.apache.hadoop.hbase.ipc.BadAuthException.class,
		org.cloudera.htrace.HTraceConfiguration.class })
public abstract class AbstractHBaseTransactionalPutOutputOperator<T>
		extends
		AbstractAggregateTransactionableStoreOutputOperator<T, HBaseTransactionalStore> {
	private static final transient Logger logger = LoggerFactory
			.getLogger(AbstractHBaseTransactionalPutOutputOperator.class);
	private transient List<T> tuples;

	public AbstractHBaseTransactionalPutOutputOperator() {
		store = new HBaseTransactionalStore();
		tuples = new ArrayList<T>();
	}

	@Override
	public void storeAggregate() {

		Iterator<T> it = tuples.iterator();
		while (it.hasNext()) {
			T t = it.next();
			try {
				HTable table = store.getTable();
				Put put = operationPut(t);
				table.put(put);
			} catch (IOException e) {
				logger.error("Could not output tuple", e);
				DTThrowable.rethrow(e);
			}
			it.remove();
		}
	}

	public abstract Put operationPut(T t) throws IOException;

	@Override
	public void processTuple(T tuple) {
		tuples.add(tuple);
	}

}
