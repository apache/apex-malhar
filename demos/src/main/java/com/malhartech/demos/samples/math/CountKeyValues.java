package com.malhartech.demos.samples.math;

import java.util.Random;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import com.malhartech.common.KeyValPair;
/**
 * Input port operator for generating random values on keys. <br>
 * Key(s) : key1, key2, key3, key4, key5. <br>
 * 
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class CountKeyValues implements InputOperator
{

	public final transient DefaultOutputPort<KeyValPair<String, Integer>> outport = new DefaultOutputPort<KeyValPair<String, Integer>>(
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
