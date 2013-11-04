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

import com.datatorrent.contrib.machinedata.data.MachineKey;

import java.util.Calendar;

/**
 * <p>AlertKey class.</p>
 *
 * @since 0.3.5
 */
public class AlertKey extends MachineKey {

    public AlertKey() {
    }

    public AlertKey(String timeKey, int day) {
        super(timeKey,day);
    }

    public AlertKey(String timeKey, int day,  Integer customer, Integer product, Integer os, Integer software1, Integer software2, Integer software3,Integer deviceId) {
        super(timeKey,day,  customer, product, os, software1, software2, software3,deviceId);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("|alert:true");
        return sb.toString();
    }
}
