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

import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.datatorrent.lib.db.AbstractStoreInputOperator;
/**
 * Base Accumulo input adapter operator, which reads data from persistence database and writes into output port(s).
 *
 * <p>
 * This is an abstract class. Sub-classes need to implement {@link #getScanner()} and {@link #getTuple(Entry)}.
 * </p>
 *
 */
public abstract class AbstractAccumuloInputOperator<T> extends
		AbstractStoreInputOperator<T, AccumuloStore> {

	public abstract T getTuple(Entry<Key, Value> entry);

	public abstract Scanner getScanner(Connector conn);

	public AbstractAccumuloInputOperator() {
		store = new AccumuloStore();
	}

	@Override
	public void emitTuples() {
		Connector conn = getStore().getConnector();
		Scanner scan = getScanner(conn);

		for (Entry<Key, Value> entry : scan) {
			T tuple = getTuple(entry);
			outputPort.emit(tuple);
		}

	}

}