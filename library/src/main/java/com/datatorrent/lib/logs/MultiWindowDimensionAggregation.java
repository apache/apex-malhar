package com.datatorrent.lib.logs;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang.mutable.MutableInt;

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

	private Map<Integer, Map<String, Number>> cacheOject;
	private Map<String,Number> countMap;

	public final transient DefaultOutputPort<Map<String,DimensionObject<String>>> output = new DefaultOutputPort<Map<String,DimensionObject<String>>>();
	
	public final transient DefaultInputPort<Map<String, String>> data = new DefaultInputPort<Map<String,String>>() {
		@Override
		public void process(Map<String,String> tuple) {
			if(tuple.get(dimension) != null){
				Map<String,Number> cacheMap = cacheOject.get(currentWindow);
				if(cacheMap == null){
					cacheMap = new HashMap<String, Number>();
					cacheOject.put(currentWindow, cacheMap);
				}
				String value = tuple.get(dimension);
				StringTokenizer tokenizer = new StringTokenizer(value, stringDelimiter);
				while(tokenizer.hasMoreTokens()){
					String token = tokenizer.nextToken();
					StringTokenizer tokenTokenizer = new StringTokenizer(token,tokenDelimiter);
					String dimensionVal = tokenTokenizer.nextToken();
					int dimensionCount = Integer.parseInt(tokenTokenizer.nextToken());
					Number n = cacheMap.get(dimensionVal);
					if(n == null){
						cacheMap.put(dimensionVal, new MutableInt(dimensionCount));
					}else{
						((MutableInt)n).add(dimensionCount);
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
		cacheOject = new HashMap<Integer, Map<String, Number>>(windowSize);
		countMap = new HashMap<String, Number>();
	}

	@Override
	public void teardown() {

	}

	@Override
	public void beginWindow(long arg0) {
		Map<String,Number> cacheMap = cacheOject.get(currentWindow);
		if(cacheMap == null)
			cacheMap = new HashMap<String, Number>();
		cacheMap.clear();
		countMap.clear();

	}

	@Override
	public void endWindow() {
		Collection<Map<String,Number>> coll = cacheOject.values();
		Iterator< Map<String,Number>> itr = coll.iterator();
		while(itr.hasNext()){
			Map<String,Number> map = itr.next();
			for(Map.Entry<String, Number> e :map.entrySet()){
				Number n = countMap.get(e.getKey());
				if(n == null){
					countMap.put(e.getKey(), new MutableInt(e.getValue()));
				}else{
					((MutableInt) n).add(e.getValue());
				}
			}
		}
		
		for(Map.Entry<String, Number> e :countMap.entrySet()){
			HashMap<String, DimensionObject<String>> outputData = new HashMap<String, DimensionObject<String>>();
			outputData.put(dimension, new DimensionObject<String>(e.getValue().intValue(), e.getKey()));
			output.emit(outputData);
		}
		
		currentWindow = (currentWindow+1)%windowSize;

	}

}
