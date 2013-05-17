package com.malhartech.demos.samples.math;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;

/**
 * Input operator to generate hash map on single value, required for sample Script operator application.
 * 
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 *
 */
public class SingleKeyValMap implements InputOperator
{

	public final transient DefaultOutputPort<Map<String, Object>> outport = new DefaultOutputPort<Map<String, Object>>(
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
