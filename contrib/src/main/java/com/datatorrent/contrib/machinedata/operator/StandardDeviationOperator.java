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

import java.util.List;
import java.util.Map;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TimeBucketKey;
import com.datatorrent.contrib.machinedata.MachineInfo;
import com.datatorrent.contrib.machinedata.MachineInfo.RESOURCE_TYPE;
import com.datatorrent.contrib.machinedata.MachineKey;
import com.datatorrent.contrib.machinedata.util.DataTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * <p>Emits standard deviation of cpu/hdd/ram per {@link MachineKey}<br>
 * Partitionable: yes<br></p>
 *
 * @since 0.3.5
 */
public class StandardDeviationOperator extends BaseOperator {
	private final DataTable<TimeBucketKey,RESOURCE_TYPE,Double> averageData = new DataTable<TimeBucketKey,RESOURCE_TYPE,Double>();
	private final DataTable<TimeBucketKey,RESOURCE_TYPE,List<Integer>> data= new DataTable<TimeBucketKey,RESOURCE_TYPE,List<Integer>>();

	@InputPortFieldAnnotation(name="dataPort")
	public transient DefaultInputPort<MachineInfo> dataPort= new MachineInfoInputPort();

	public transient DefaultInputPort<KeyValPair<TimeBucketKey, Map<String, Double>>> averagePort= new DefaultInputPort<KeyValPair<TimeBucketKey,
			Map<String, Double>>>() {

				@Override
				public void process(
						KeyValPair<TimeBucketKey, Map<String, Double>> tuple) {
					averageData.put(tuple.getKey(), RESOURCE_TYPE.CPU, tuple.getValue().get("cpu"));
					averageData.put(tuple.getKey(), RESOURCE_TYPE.HDD, tuple.getValue().get("hdd"));
					averageData.put(tuple.getKey(), RESOURCE_TYPE.RAM, tuple.getValue().get("ram"));
				}
	};

    @Override
    public void setup(OperatorContext context){
    	((MachineInfoInputPort)dataPort).setDataCache(data);
    }

    @OutputPortFieldAnnotation(name="outputPort")
    public final transient DefaultOutputPort<KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE, Double>>> outputPort =
            new DefaultOutputPort<KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE, Double>>>();


    @Override
    public void endWindow() {

    	for (TimeBucketKey machineKey: data.rowKeySet()){

    		Map<RESOURCE_TYPE, Double> sdData= Maps.newHashMap();
    		sdData.put(RESOURCE_TYPE.CPU, getSD(data.get(machineKey, RESOURCE_TYPE.CPU), averageData.get(machineKey, RESOURCE_TYPE.CPU)));
    		sdData.put(RESOURCE_TYPE.RAM, getSD(data.get(machineKey, RESOURCE_TYPE.RAM), averageData.get(machineKey, RESOURCE_TYPE.RAM)));
    		sdData.put(RESOURCE_TYPE.HDD, getSD(data.get(machineKey, RESOURCE_TYPE.HDD), averageData.get(machineKey, RESOURCE_TYPE.HDD)));

    		outputPort.emit(new KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE,Double>>(machineKey, sdData));
    	}
    	data.clear();
    	averageData.clear();
    }

    private double getSD(List<Integer> data, double avg) {
    	Preconditions.checkNotNull(data);
    	double sd=0;
    	for(Integer point: data){
    		sd+= Math.pow(point-avg,2);
    	}
    	return Math.sqrt(sd);
    }


}
