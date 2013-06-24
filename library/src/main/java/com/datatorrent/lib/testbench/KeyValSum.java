/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.datatorrent.lib.testbench;

import java.text.ParseException;
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
public class KeyValSum extends BaseOperator
{
	private Map<String, Integer> collect;
	public final transient DefaultInputPort<Map<String, Integer>> inport = new DefaultInputPort<Map<String, Integer>>() {
    @Override
    public void process(Map<String, Integer> s) {
    	for(Map.Entry<String, Integer> entry : s.entrySet())
    	{
	    	if (collect.containsKey(entry.getKey()))
	    	{
	    		Integer value = (Integer)collect.remove(entry.getKey());
	    		collect.put(entry.getKey(), value + entry.getValue());
	    	} else {
	    		collect.put(entry.getKey(), entry.getValue());
	    	}
    	}
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
		collect  = new HashMap<String, Integer>();
	}
	
	// out port
	public final transient DefaultOutputPort<Map<String, Integer>> outport = new DefaultOutputPort<Map<String, Integer>>();
	
	@Override
	public void endWindow()
	{
		outport.emit(collect);
	}
}
