/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.machinedata.operator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TimeBucketKey;
import com.datatorrent.contrib.machinedata.MachineInfo;
import com.datatorrent.contrib.machinedata.MachineInfo.RESOURCE_TYPE;
import com.datatorrent.contrib.machinedata.MachineKey;
import com.datatorrent.contrib.machinedata.util.DataTable;
import com.google.common.collect.Maps;

/**
 * <p>Emits kth percentile of cpu/ram/hdd per {@link MachineKey} <br>
 * Partionable: yes <br></p>
 *
 * @since 0.3.5
 */
public class PercentileOperator extends BaseOperator {

	private final DataTable<TimeBucketKey,RESOURCE_TYPE,List<Integer>> data= new DataTable<TimeBucketKey,RESOURCE_TYPE,List<Integer>>();

	@Min(1)
	@Max(99)
	private int kthPercentile =95; //kth percentile

	@InputPortFieldAnnotation(name="dataPort")
	public final transient DefaultInputPort<MachineInfo> dataPort = new MachineInfoInputPort();

    @OutputPortFieldAnnotation(name="outputPort")
    public final transient DefaultOutputPort<KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE, Double>>> outputPort =
            new DefaultOutputPort<KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE, Double>>>();

    @Override
    public void setup(OperatorContext context){
    	((MachineInfoInputPort)dataPort).setDataCache(data);
    }

    @Override
    public void endWindow() {

    	for (TimeBucketKey machineKey: data.rowKeySet()){
    		Collections.sort(data.get(machineKey, RESOURCE_TYPE.CPU));
    		Collections.sort(data.get(machineKey, RESOURCE_TYPE.RAM));
    		Collections.sort(data.get(machineKey, RESOURCE_TYPE.HDD));

    		Map<RESOURCE_TYPE, Double> percentileData= Maps.newHashMap();
    		percentileData.put(RESOURCE_TYPE.CPU, getKthPercentile(data.get(machineKey, RESOURCE_TYPE.CPU)));
    		percentileData.put(RESOURCE_TYPE.RAM, getKthPercentile(data.get(machineKey, RESOURCE_TYPE.RAM)));
    		percentileData.put(RESOURCE_TYPE.HDD, getKthPercentile(data.get(machineKey, RESOURCE_TYPE.HDD)));
    		outputPort.emit(new KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE,Double>>(machineKey, percentileData));
    	}
    	data.clear();
    }


    private double getKthPercentile(List<Integer> sorted){

    	double val = (kthPercentile * sorted.size())/100.0;
    	if(val== (int) val){
    		//Whole number
    		int idx = (int) val-1;
    		return (sorted.get(idx) + sorted.get(idx+1))/2.0;
    	}
    	else{
    		int idx=(int) Math.round(val)-1;
    		return sorted.get(idx);
    	}
    }

    /**
     *
     * @param kVal the percentile which will be emitted by this operator
     */
    public void setKthPercentile(int kVal){
    	this.kthPercentile=kVal;
    }
}
