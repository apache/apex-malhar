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
package com.datatorrent.samples.lib.math;

import java.util.Random;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.KeyValPair;
/**
 * Input port operator for generating random values on keys. <br>
 * Key(s) : key1, key2, key3, key4, key5. <br>
 *
 * @since 0.3.2
 */
public class CountKeyValues implements InputOperator
{

	public final transient DefaultOutputPort<KeyValPair<String, Integer>> outport = new DefaultOutputPort<KeyValPair<String, Integer>>();
	private Random random = new Random(11111);

	@Override
	public void beginWindow(long windowId)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void endWindow()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setup(OperatorContext context)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void teardown()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void emitTuples()
	{
		outport.emit(new KeyValPair<String, Integer>("key1", getNextInt()));
		outport.emit(new KeyValPair<String, Integer>("key2", getNextInt()));
		outport.emit(new KeyValPair<String, Integer>("key3", getNextInt()));
		outport.emit(new KeyValPair<String, Integer>("key4", getNextInt()));
		outport.emit(new KeyValPair<String, Integer>("key5", getNextInt()));
		try
		{
			Thread.sleep(500);
		} catch (Exception e)
		{
		}
	}

	private int getNextInt()
	{
		int value = Math.abs(random.nextInt()) % 100;
		return value;
	}
}
