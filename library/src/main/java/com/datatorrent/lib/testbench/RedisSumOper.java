/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.datatorrent.lib.testbench;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Context.OperatorContext;

/**
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class RedisSumOper extends BaseOperator
{
	private ArrayList<Integer> collect;
	public final transient DefaultInputPort<Integer> inport = new DefaultInputPort<Integer>() {
	    @Override
	    public void process(Integer s) {
	      collect.add(s);
	    }
	};

	@Override
	public void setup(OperatorContext context)
	{
	}

	@Override
	public void teardown()
	{
	}

	@Override
	public void beginWindow(long windowId)
	{
		collect  = new ArrayList<Integer>();
	}
	
	// out port
	public final transient DefaultOutputPort<Map<Integer, Integer>> outport = new DefaultOutputPort<Map<Integer, Integer>>();
	
	@Override
	public void endWindow()
	{
		Integer sum = 0;
		for(Integer entry : collect) sum += entry;
		Map<Integer, Integer> tuple = new HashMap<Integer, Integer>();
		tuple.put(1, sum);
		outport.emit(tuple);
	}
}
