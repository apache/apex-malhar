package com.datatorrent.lib.samplecode.math;

import java.util.Random;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.Pair;

/**
 * Input port operator for generating random values Pair for sample application.
 * 
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class RandomPairIntegers implements InputOperator
{

	public final transient DefaultOutputPort<Pair<Integer, Integer>> outport = new DefaultOutputPort<Pair<Integer, Integer>>(
			this);
	private Random random = new Random(11111);
	private boolean equal = false;

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
		if (equal)
		{
			int val = random.nextInt();
			outport.emit(new Pair<Integer, Integer>(new Integer(val),
					new Integer(val)));
		} else
		{
			outport.emit(new Pair<Integer, Integer>(new Integer(random.nextInt()),
					new Integer(random.nextInt())));
		}
		equal = !equal;
	}

}
