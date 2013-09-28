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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.lib.util.KeyValPair;

import java.util.*;

/**
 * <p>Information tuple generator with randomness.</p>
 *
 * @since 0.3.5
 */
public class RandomInformationTupleGenerator extends BaseOperator implements InputOperator {


    public transient DefaultOutputPort<CpuInfo> cpu = new DefaultOutputPort<CpuInfo>();
    public transient DefaultOutputPort<RamInfo> ram = new DefaultOutputPort<RamInfo>();
    public transient DefaultOutputPort<HddInfo> hdd = new DefaultOutputPort<HddInfo>();

    public transient DefaultOutputPort<MachineInfo> outputInline = new DefaultOutputPort<MachineInfo>();
    public transient DefaultOutputPort<MachineInfo> output = new DefaultOutputPort<MachineInfo>();

    public transient DefaultOutputPort<KeyValPair<AlertKey, Map<String, Integer>>> alert =
            new DefaultOutputPort<KeyValPair<AlertKey, Map<String, Integer>>>();
    public transient DefaultOutputPort<String> smtpAlert = new DefaultOutputPort<String>();

    private final Random randomCustomerId = new Random();
    private final Random randomProductVer = new Random();
    private final Random randomOSVer = new Random();
    private final Random randomSoftware3Ver = new Random();
    private final Random randomSoftware1Ver = new Random();
    private final Random randomSoftware2Ver = new Random();
    private final Random randomCpu = new Random();
    private final Random randomRam = new Random();
    private final Random randomHdd = new Random();

    private final Random randomNum = new Random();

    private int customerMin = 1;
    private int customerMax = 5;
    private int productMin = 4;
    private int productMax = 6;
    private int osMin = 10;
    private int osMax = 12;
    private int software1Min = 10;
    private int software1Max = 12;
    private int software2Min = 12;
    private int software2Max = 14;
    private int software3Min = 4;
    private int software3Max = 6;
    private int cpuMin = 10;
    private int cpuMax = 70;
    private int ramMin = 10;
    private int ramMax = 70;
    private int hddMin = 10;
    private int hddMax = 70;

    private int randMin = 0;
    private int randMax = 63;

//    private int tupleBlastSize = 50;
    private int tupleBlastSize = 10000;

    private boolean genAlert;
    private int cpuThreshold = 70;
    private int ramThreshold = 70;
    private int hddThreshold = 90;

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
    }

    @Override
    public void emitTuples() {
        int count = 0;
        while (count < tupleBlastSize) {
//            Integer[] values = new Integer[6];
//            values[0] = genCustomerId();
//            values[1] = genProductVer();
//            values[2] = genOsVer();
//            values[3] = genSoftware3Ver();
//            values[4] = genSoftware1Ver();
//            values[5] = genSoftware2Ver();

//            Combinatorics<Integer> combinatorics = new Combinatorics<Integer>(values);
//            Map<Integer, List<Integer>> map = combinatorics.generate();
//            Set<Integer> keys = map.keySet();
//            Iterator<Integer> keysIter = keys.iterator();
//            while (keysIter.hasNext()) {
//                Integer key = keysIter.next();
//                List<Integer> combination = map.get(key);
//                for (int id: combination) {
//
//                }
//            }

            Calendar calendar = Calendar.getInstance();

            int customerVal = genCustomerId();
            int productVal = genProductVer();
            int osVal = genOsVer();
            int software1Val = genSoftware1Ver();
            int software2Val = genSoftware2Ver();
            int software3Val = genSoftware3Ver();

            int cpuVal = genCpu();
            int ramVal = genRam();
            int hddVal = genHdd();

            for (int i = 0; i < 64; i++) {
                MachineKey machineKey = new MachineKey(calendar, MachineKey.TIMESPEC_MINUTE_SPEC);
                if ((i & 1) != 0) machineKey.setCustomer(customerVal);
                if ((i & 2) != 0) machineKey.setProduct(productVal);
                if ((i & 4) != 0) machineKey.setOs(osVal);
                if ((i & 8) != 0) machineKey.setSoftware1(software1Val);
                if ((i & 16) != 0) machineKey.setSoftware2(software2Val);
                if ((i & 32) != 0) machineKey.setSoftware3(software3Val);

                MachineInfo machineInfo = new MachineInfo();
                machineInfo.setMachineKey(machineKey);
                machineInfo.setCpu(cpuVal);
                machineInfo.setRam(ramVal);
                machineInfo.setHdd(hddVal);

                outputInline.emit(machineInfo);
                output.emit(machineInfo);

//                if (i % 16 == 0) { // generate an error tuple
//
//                    machineKey = new MachineKey(calendar, MachineKey.TIMESPEC_MINUTE_SPEC);
//                    machineKey.setCustomer(1);
//                    machineKey.setProduct(5);
//                    machineKey.setOs(10);
//                    machineKey.setSoftware1(12);
//                    machineKey.setSoftware2(14);
//                    machineKey.setSoftware3(6);
//
//                    machineInfo = new MachineInfo();
//                    machineInfo.setMachineKey(machineKey);
//                    machineInfo.setCpu(100);
//                    machineInfo.setRam(20);
//                    machineInfo.setHdd(200);
//
//                    machine.emit(machineInfo);
//                }
            }

            count++;
        }
    }

    public int genCustomerId() {
        int range = customerMax - customerMin + 1;
        return customerMin + randomCustomerId.nextInt(range);
    }

    public int genProductVer() {
        int range = productMax - productMin + 1;
        return productMin + randomProductVer.nextInt(range);
    }

    public int genOsVer() {
        int range = osMax - osMin + 1;
        return osMin + randomOSVer.nextInt(range);
    }

    public int genSoftware3Ver() {
        int range = software3Max - software3Min + 1;
        return software3Min + randomSoftware3Ver.nextInt(range);
    }
    public int genSoftware1Ver() {
        int range = software1Max - software1Min + 1;
        return software1Min + randomSoftware1Ver.nextInt(range);
    }
    public int genSoftware2Ver() {
        int range = software2Max - software2Min + 1;
        return software2Min + randomSoftware2Ver.nextInt(range);
    }
    public int genCpu() {
        int range = cpuMax - cpuMin + 1;
        return cpuMin + randomCpu.nextInt(range);
    }
    public int genRam() {
        int range = ramMax - ramMin + 1;
        return ramMin + randomRam.nextInt(range);
    }
    public int genHdd() {
        int range = hddMax - hddMin + 1;
        return hddMin + randomHdd.nextInt(range);
    }

    public int genRandom() {
        int range = randMax - randMin + 1;
        return randMin + randomNum.nextInt(range);
    }

    public void setGenAlert(boolean genAlert) {
        Calendar calendar = Calendar.getInstance();
        long timestamp = System.currentTimeMillis();
        calendar.setTimeInMillis(timestamp);

        AlertKey alertKey = new AlertKey(calendar, MachineKey.TIMESPEC_MINUTE_SPEC);
        alertKey.setCustomer(1);
        alertKey.setProduct(5);
        alertKey.setOs(10);
        alertKey.setSoftware1(12);
        alertKey.setSoftware2(14);
        alertKey.setSoftware3(6);

        MachineInfo machineInfo = new MachineInfo();
        machineInfo.setMachineKey(alertKey);
        machineInfo.setCpu(cpuThreshold + 1);
        machineInfo.setRam(ramThreshold + 1);
        machineInfo.setHdd(hddThreshold + 1);

        outputInline.emit(machineInfo);
        output.emit(machineInfo);

        Map<String, Integer> valueMap = new HashMap<String, Integer>();
        valueMap.put("cpu", machineInfo.getCpu());
        valueMap.put("ram", machineInfo.getRam());
        valueMap.put("hdd", machineInfo.getHdd());
        alert.emit(new KeyValPair<AlertKey, Map<String, Integer>>(alertKey, valueMap));
        int cval = cpuThreshold + 1;
        int rval = ramThreshold + 1;
        int hval = hddThreshold + 1;
        smtpAlert.emit("CPU Alert: CPU Usage threshold (" + cpuThreshold + ") breached: current % usage: " + cval);
        smtpAlert.emit("RAM Alert: RAM Usage threshold (" + ramThreshold + ") breached: current % usage: " + rval);
        smtpAlert.emit("HDD Alert: HDD Usage threshold (" + hddThreshold + ") breached: current % usage: " + hval);
    }

    public int getCpuThreshold() {
        return cpuThreshold;
    }

    public void setCpuThreshold(int cpuThreshold) {
        this.cpuThreshold = cpuThreshold;
    }

    public int getRamThreshold() {
        return ramThreshold;
    }

    public void setRamThreshold(int ramThreshold) {
        this.ramThreshold = ramThreshold;
    }

    public int getHddThreshold() {
        return hddThreshold;
    }

    public void setHddThreshold(int hddThreshold) {
        this.hddThreshold = hddThreshold;
    }
}

