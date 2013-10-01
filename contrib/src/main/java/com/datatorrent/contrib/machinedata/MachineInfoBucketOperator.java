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
package com.datatorrent.contrib.machinedata;

import com.datatorrent.api.*;
import com.datatorrent.lib.util.KeyValPair;

/**
 * <p>MachineInfoBucketOperator class.</p>
 *
 * @since 0.3.5
 */
public class MachineInfoBucketOperator extends BaseOperator {

    public final transient DefaultOutputPort<KeyValPair<MachineKey, Integer>> cpuOutputPort =
            new DefaultOutputPort<KeyValPair<MachineKey, Integer>>();
    public final transient DefaultOutputPort<KeyValPair<MachineKey, Integer>> ramOutputPort =
            new DefaultOutputPort<KeyValPair<MachineKey, Integer>>();
    public final transient DefaultOutputPort<KeyValPair<MachineKey, Integer>> hddOutputPort =
            new DefaultOutputPort<KeyValPair<MachineKey, Integer>>();

    public transient DefaultInputPort<CpuInfo> cpuInputPort = new DefaultInputPort<CpuInfo>() {
        @Override
        public void process(CpuInfo tuple) {
            cpuOutputPort.emit(new KeyValPair<MachineKey, Integer>(tuple.getMachineKey(), tuple.getCpu()));
        }
    };

    public transient DefaultInputPort<RamInfo> ramInputPort = new DefaultInputPort<RamInfo>() {
        @Override
        public void process(RamInfo tuple) {
            ramOutputPort.emit(new KeyValPair<MachineKey, Integer>(tuple.getMachineKey(), tuple.getRam()));
        }
    };

    public transient DefaultInputPort<HddInfo> hddInputPort = new DefaultInputPort<HddInfo>() {
        @Override
        public void process(HddInfo tuple) {
            hddOutputPort.emit(new KeyValPair<MachineKey, Integer>(tuple.getMachineKey(), tuple.getHdd()));
        }
    };
}
