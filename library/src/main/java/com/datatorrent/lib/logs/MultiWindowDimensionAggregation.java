package com.datatorrent.lib.logs;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

public class MultiWindowDimensionAggregation implements Operator {

	private int windowSize = 2;
	private int currentWindow=0;
	private String dimension;
	private String stringDelimiter = ":";
	private String tokenDelimiter="=";

	private Map<Integer, Map<String, Integer>> cacheOject;
	private Map<String,Integer> countMap;

	public final transient DefaultOutputPort<Map<String,DimensionObject<String>>> output = new DefaultOutputPort<Map<String,DimensionObject<String>>>();
	
	public final transient DefaultInputPort<Map<String, String>> data = new DefaultInputPort<Map<String,String>>() {
		@Override
		public void process(Map<String,String> tuple) {
			if(tuple.get(dimension) != null){
				Map<String,Integer> cacheMap = cacheOject.get(currentWindow);
				if(cacheMap == null){
					cacheMap = new HashMap<String, Integer>();
					cacheOject.put(currentWindow, cacheMap);
				}
				String value = tuple.get(dimension);
				StringTokenizer tokenizer = new StringTokenizer(value, stringDelimiter);
				while(tokenizer.hasMoreTokens()){
					String token = tokenizer.nextToken();
					StringTokenizer tokenTokenizer = new StringTokenizer(token,tokenDelimiter);
					String dimensionVal = tokenTokenizer.nextToken();
					int dimensionCount = Integer.parseInt(tokenTokenizer.nextToken());
					if(cacheMap.get(dimensionVal) == null){
						cacheMap.put(dimensionVal, dimensionCount);
					}else{
						dimensionCount = dimensionCount + cacheMap.get(dimensionVal);
						cacheMap.put(dimensionVal,dimensionCount);
					}
				}				
			}
			
		}
	};

	public String getDelimiter() {
		return stringDelimiter;
	}

	public void setDelimiter(String delimiter) {
		this.stringDelimiter = delimiter;
	}
	public String getDimension() {
		return dimension;
	}

	public void setDimension(String dimension) {
		this.dimension = dimension;
	}

	public int getWindowSize() {
		return windowSize;
	}

	public void setWindowSize(int windowSize) {
		this.windowSize = windowSize;
	}

	@Override
	public void setup(OperatorContext arg0) {
		cacheOject = new HashMap<Integer, Map<String, Integer>>(windowSize);
		countMap = new HashMap<String, Integer>();
	}

	@Override
	public void teardown() {

	}

	@Override
	public void beginWindow(long arg0) {
		Map<String,Integer> cacheMap = cacheOject.get(currentWindow);
		if(cacheMap == null)
			cacheMap = new HashMap<String, Integer>();
		cacheMap.clear();
		countMap.clear();

	}

	@Override
	public void endWindow() {
		Collection<Map<String,Integer>> coll = cacheOject.values();
		Iterator< Map<String,Integer>> itr = coll.iterator();
		while(itr.hasNext()){
			Map<String,Integer> map = itr.next();
			for(Map.Entry<String, Integer> e :map.entrySet()){
				if(countMap.get(e.getKey()) == null){
					countMap.put(e.getKey(), e.getValue());
				}else{
					int count = countMap.get(e.getKey());
					countMap.put(e.getKey(), e.getValue()+count);
				}
			}
		}
		
		for(Map.Entry<String, Integer> e :countMap.entrySet()){
			HashMap<String, DimensionObject<String>> outputData = new HashMap<String, DimensionObject<String>>();
			outputData.put(dimension, new DimensionObject<String>(e.getValue(), e.getKey()));
			output.emit(outputData);
		}
		
		currentWindow = (currentWindow+1)%windowSize;

	}

}
