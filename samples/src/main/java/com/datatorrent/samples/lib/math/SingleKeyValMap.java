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
package com.datatorrent.samples.lib.math;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

/**
 * Input operator to generate hash map on single value, required for sample Script operator application.
 *
 * @since 0.3.2
 */
public class SingleKeyValMap implements InputOperator
{

	public final transient DefaultOutputPort<Map<String, Object>> outport = new DefaultOutputPort<Map<String, Object>>();
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
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("val", random.nextInt());
		outport.emit(map);
		try
		{
			Thread.sleep(500);
		} catch (Exception e)
		{
		}
	}

}
