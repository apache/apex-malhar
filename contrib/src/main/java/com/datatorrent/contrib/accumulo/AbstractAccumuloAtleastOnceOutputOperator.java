/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.accumulo;

import java.util.List;

import javax.validation.constraints.Min;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractPassThruTransactionableStoreOutputOperator;
import com.google.common.collect.Lists;

/**
 * Operator for storing tuples in Accumulo rows.<br>
 * 
 * <br>
 * This class provides a Accumulo output operator that can be used to store
 * tuples in rows in a Accumulo table. It should be extended to provide specific
 * implementation. The extending class should implement operationMutation method
 * and provide a Accumulo Mutation metric object that specifies where and what
 * to store for the tuple in the table.<br>
 * 
 * <br>
 * This class provides a batch put where tuples are collected till the end
 * window and they are put on end window
 * 
 * Note that since Accumulo doesn't support transactions this store cannot
 * guarantee each tuple is written only once to Accumulo in case the operator is
 * restarted from an earlier checkpoint. It only tries to minimize the number of
 * duplicates limiting it to the tuples that were processed in the window when
 * the operator shutdown.
 * BenchMark Results
 * -----------------
 * The operator operates at 30,000 tuples/sec with the following configuration
 * 
 * Container memory size=1G
 * Accumulo Max Memory=2G
 * Accumulo number of write threads=1
 * CPU=Intel(R) Core(TM) i7-4500U CPU @ 1.80 GHz 2.40 Ghz
 * 
 * @param <T>
 *            The tuple type
 */

public abstract class AbstractAccumuloAtleastOnceOutputOperator<T> extends AbstractPassThruTransactionableStoreOutputOperator<T, AccumuloTransactionalStore> {
	protected static int DEFAULT_BATCH_SIZE = 1000;
	private static final transient Logger logger = LoggerFactory
			.getLogger(AbstractAccumuloAtleastOnceOutputOperator.class);

	@Min(1)
	private int batchSize;
	private final List<T> tuples;
	public AbstractAccumuloAtleastOnceOutputOperator()
	{
		tuples = Lists.newArrayList();
		batchSize = DEFAULT_BATCH_SIZE;
		store = new AccumuloTransactionalStore();
	}
	@Override
	public void processTuple(T tuple)
	{
		tuples.add(tuple);
		if (tuples.size() >= batchSize) {
			processBatch();
		}

	}
	@Override
	public void endWindow()
	{
		if (tuples.size() > 0) {
			processBatch();
		}
		super.endWindow();
	}

	/**
	 * Sets the size of a batch operation.<br/>
	 * <b>Default:</b> {@value #DEFAULT_BATCH_SIZE}
	 *
	 * @param batchSize size of a batch
	 */
	public void setBatchSize(int batchSize)
	{
		this.batchSize = batchSize;
	}
	/**
	 * 
	 * @param t
	 * @return Mutation
	 */
	public abstract Mutation operationMutation(T t);

	private void processBatch()
	{
		try {
			for (T tuple : tuples) {
				Mutation mutation = operationMutation(tuple);
				store.getBatchwriter().addMutation(mutation);
			}
			store.getBatchwriter().flush();

		} catch (MutationsRejectedException e) {
			logger.error("unable to write mutations", e);
			DTThrowable.rethrow(e);
		}
		tuples.clear();
	}
}

