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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.util.TimeBucketKey;
import com.datatorrent.contrib.machinedata.MachineInfo;
import com.datatorrent.contrib.machinedata.MachineInfo.RESOURCE_TYPE;
import com.datatorrent.contrib.machinedata.MachineKey;
import com.datatorrent.contrib.machinedata.util.DataTable;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 * <p>Common implementation of input port which stores list of all tuples per machine key<br>
 * It uses sticky partitioning.</p>
 *
 * @since 0.3.5
 */
public class MachineInfoInputPort extends DefaultInputPort<MachineInfo> {
	private static final Logger logger = LoggerFactory.getLogger(MachineInfoInputPort.class);

	@Nonnull private DataTable<TimeBucketKey,RESOURCE_TYPE,List<Integer>> dataCache;

	public void setDataCache(DataTable<TimeBucketKey,RESOURCE_TYPE,List<Integer>> cache){
		dataCache=cache;
	}

	public DataTable<TimeBucketKey,RESOURCE_TYPE,List<Integer>> getDataCache(){
		return dataCache;
	}

	@Override
	public void process(MachineInfo tuple) {
        MachineKey machineKey = tuple.getMachineKey();
        if(!dataCache.containsRow(machineKey)) {
        	dataCache.put(machineKey, RESOURCE_TYPE.CPU, Lists.<Integer>newArrayList());
        	dataCache.put(machineKey, RESOURCE_TYPE.RAM, Lists.<Integer>newArrayList());
        	dataCache.put(machineKey, RESOURCE_TYPE.HDD, Lists.<Integer>newArrayList());
        }
        dataCache.get(machineKey, RESOURCE_TYPE.CPU).add(tuple.getCpu());
        dataCache.get(machineKey, RESOURCE_TYPE.RAM).add(tuple.getRam());
        dataCache.get(machineKey, RESOURCE_TYPE.HDD).add(tuple.getHdd());
	}


    /**
     * Stream codec used for partitioning.
     */
    @Override
    public Class<? extends StreamCodec<MachineInfo>> getStreamCodec(){
      return getMachineInfoStreamCodec();
    }

    private Class<? extends StreamCodec<MachineInfo>> getMachineInfoStreamCodec(){
    	return MachineInfoStreamCodec.class;
    }

    public static class MachineInfoStreamCodec implements StreamCodec<MachineInfo>{
    	private final Kryo kryo;

    	public MachineInfoStreamCodec(){
    		this.kryo=new Kryo();
    		this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    	}

		@Override
		public MachineInfo fromByteArray(Slice fragment) {
			ByteArrayInputStream is= new ByteArrayInputStream(fragment.buffer,fragment.offset, fragment.length);
			Input input = new Input(is);
			MachineInfo info= kryo.readObject(input, MachineInfo.class);
			return info;
		}

		@Override
		public Slice toByteArray(MachineInfo info) {
			ByteArrayOutputStream os=new ByteArrayOutputStream();
			Output output = new Output(os);
			kryo.writeObject(output, info);
			output.flush();
			return new Slice(os.toByteArray(),0, os.toByteArray().length);
		}

		@Override
		public int getPartition(MachineInfo o) {
			return Objects.hashCode(o.getMachineKey().getCustomer(), o.getMachineKey().getOs(), o.getMachineKey().getProduct(),
					o.getMachineKey().getSoftware1(), o.getMachineKey().getSoftware2(), o.getMachineKey().getSoftware3());
		}
    }
}
