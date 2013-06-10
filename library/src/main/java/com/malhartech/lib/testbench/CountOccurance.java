/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator;

/**
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class CountOccurance<k> extends BaseOperator
{
	private Map<k, Integer> collect;
	public final transient DefaultInputPort<k> inport = new DefaultInputPort<k>(this) {
    @Override
    public void process(k s) {
    	if (collect.containsKey(s))
    	{
    		Integer value = (Integer)collect.remove(s);
    		collect.put(s, new Integer(value+1));
    	} else {
    		collect.put(s, new Integer(1));
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
		collect  = new HashMap<k, Integer>();
	}
	
	// out port
	public final transient DefaultOutputPort<Map<k, Integer>> outport = new DefaultOutputPort<Map<k, Integer>>(this);
	public final transient DefaultOutputPort<Map<String, Object>> dimensionOut = new DefaultOutputPort<Map<String, Object>>(this);
	
	@Override
	public void endWindow()
	{
		outport.emit(collect);
		long timestamp = new Date().getTime();
		for(Map.Entry entry : collect.entrySet())
		{
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("timestamp", timestamp);
			map.put("item", entry.getKey());
			map.put("view", entry.getValue());
			dimensionOut.emit(map);
		}
		collect = null;
		collect  = new HashMap<k, Integer>();
	}
}
