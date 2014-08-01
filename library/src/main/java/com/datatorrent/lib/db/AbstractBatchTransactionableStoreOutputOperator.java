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
package com.datatorrent.lib.db;

import java.util.List;

import com.datatorrent.api.Context.OperatorContext;
import com.google.common.collect.Lists;

/**
 * This abstract class is for aggregate output (over one application window, all in one single batch) to a transactionable store with the transactional exactly-once feature.
 *
 * @param <T> The tuple type.
 * @param <S> The store type.
 * @since 1.0.2
 */
public abstract class AbstractBatchTransactionableStoreOutputOperator<T, S extends TransactionableStore> extends AbstractTransactionableStoreOutputOperator<T, S>{
	protected final List<T> tuples;

	public AbstractBatchTransactionableStoreOutputOperator(){
		tuples = Lists.newArrayList();
	}

	@Override
	public void setup(OperatorContext context)
	{
		super.setup(context);
	}

	@Override
	public void processTuple(T tuple)
	{
		tuples.add(tuple);
	}

	@Override
	public void endWindow()
	{
		store.beginTransaction();
		processBatch();
		store.storeCommittedWindowId(appId, operatorId, currentWindowId);
		store.commitTransaction();
		committedWindowId = currentWindowId;
		super.endWindow();
		tuples.clear();
	}

	/**
	 * Processes the whole batch at the end window and writes to the store.
	 *
	 */
	public abstract void processBatch();
}
