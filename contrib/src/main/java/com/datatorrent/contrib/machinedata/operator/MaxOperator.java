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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

/**
 * <p>Emits max of cpu/hdd/ram per {@link MachineKey}<br>
 * Partitionable: yes<br></p>
 *
 * @since 0.3.5
 */
public class MaxOperator extends BaseOperator{
	private final DataTable<TimeBucketKey, RESOURCE_TYPE, List<Integer>> data= new DataTable<TimeBucketKey, RESOURCE_TYPE, List<Integer>>();

	@InputPortFieldAnnotation(name="dataPort")
	public transient DefaultInputPort<MachineInfo> dataPort= new MachineInfoInputPort();

	@OutputPortFieldAnnotation(name = "outputPort")
	public final transient DefaultOutputPort<KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE, Integer>>> outputPort = new DefaultOutputPort<KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE, Integer>>>() {

		@Override
		public Unifier<KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE, Integer>>> getUnifier() {
			return new UnifierMaxMachineInfo();
		}

	};

    @Override
    public void setup(OperatorContext context){
    	((MachineInfoInputPort)dataPort).setDataCache(data);
    }

    @Override
    public void endWindow(){
    	for (TimeBucketKey machineKey: data.rowKeySet()){

    		Map<RESOURCE_TYPE, Integer> maxData= Maps.newHashMap();
    		maxData.put(RESOURCE_TYPE.CPU, Collections.max(data.get(machineKey,RESOURCE_TYPE.CPU)));
    		maxData.put(RESOURCE_TYPE.RAM, Collections.max(data.get(machineKey,RESOURCE_TYPE.RAM)));
    		maxData.put(RESOURCE_TYPE.HDD, Collections.max(data.get(machineKey,RESOURCE_TYPE.CPU)));

    		outputPort.emit(new KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE,Integer>>(machineKey, maxData));
    	}
    	data.clear();

    }

    public static class UnifierMaxMachineInfo implements Unifier<KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE, Integer>>>{
    	private final Table<TimeBucketKey, RESOURCE_TYPE, Integer> unifiedData= HashBasedTable.create();
    	public final transient DefaultOutputPort<KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE,Integer>>> unifiedPort = new DefaultOutputPort<KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE,Integer>>>();


		@Override
		public void beginWindow(long windowId) {
		}

		@Override
		public void endWindow() {
			for(TimeBucketKey machineKey: unifiedData.rowKeySet()){
				unifiedPort.emit(new KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE,Integer>>(machineKey, unifiedData.row(machineKey)));
			}
		}

		@Override
		public void setup(OperatorContext context) {
		}

		@Override
		public void teardown() {
		}

		@Override
		public void process(
				KeyValPair<TimeBucketKey, Map<RESOURCE_TYPE, Integer>> tuple) {
			TimeBucketKey key = tuple.getKey();
			for (RESOURCE_TYPE rType : RESOURCE_TYPE.values()) {
				if (unifiedData.get(key, rType) == null
						|| unifiedData.get(key, rType).compareTo(
								tuple.getValue().get(rType)) < 0) {
					unifiedData.put(key, rType, tuple.getValue().get(rType));
				}
			}
		}
	}
}
