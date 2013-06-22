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
public class CountOccurance<k> extends BaseOperator
{
	private Map<k, Integer> collect;
	public final transient DefaultInputPort<k> inport = new DefaultInputPort<k>() {
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
	public final transient DefaultOutputPort<Map<k, Integer>> outport = new DefaultOutputPort<Map<k, Integer>>();
	public final transient DefaultOutputPort<Map<String, Object>> dimensionOut = new DefaultOutputPort<Map<String, Object>>();
	public final transient DefaultOutputPort<Map<String,Integer>> total = new DefaultOutputPort<Map<String,Integer>>();
	
	@Override
	public void endWindow()
	{
		outport.emit(collect);
		long timestamp = new Date().getTime();
		int allcount = 0;
		for(Map.Entry<k, Integer> entry : collect.entrySet())
		{
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("timestamp", timestamp);
			map.put("item", entry.getKey());
			map.put("view", entry.getValue());
			dimensionOut.emit(map);
			allcount += entry.getValue();
		}
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("total", new Integer(allcount));
		total.emit(map);
		collect = null;
		collect  = new HashMap<k, Integer>();
	}
}
