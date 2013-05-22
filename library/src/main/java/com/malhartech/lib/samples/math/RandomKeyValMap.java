package com.malhartech.lib.samples.math;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;

/**
 * Input port operator for generating random values on key, tuples are HashMap for key/values.<br>
 * Key(s) : key1, key2, key3, key4, key5. <br>
 * 
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class RandomKeyValMap implements InputOperator
{

	public final transient DefaultOutputPort<Map<String, Integer>> outport = new DefaultOutputPort<Map<String, Integer>>(
			this);
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
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		Integer val = new Integer(Math.abs(random.nextInt()) % 100);
		Integer val1 = new Integer(Math.abs(random.nextInt()) % 100);
		map.put(val.toString(), val1);
		outport.emit(map);
		try
		{
			Thread.sleep(500);
		} catch(Exception e) {
		}
	}

}
