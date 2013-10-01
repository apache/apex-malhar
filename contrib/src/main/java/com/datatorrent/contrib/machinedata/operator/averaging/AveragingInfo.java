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
package com.datatorrent.contrib.machinedata.operator.averaging;

import com.datatorrent.lib.util.TimeBucketKey;

import java.util.Map;

/**
 * <p>AveragingInfo class.</p>
 *
 * @since 0.3.5
 */
public interface AveragingInfo<ValueKey, NumType extends Number> {

    /**
     * The key to be used, which can be used in the KeyVal operator
     *
     * @return
     */
    public TimeBucketKey getAveragingKey();

    /**
     * The map returned should contain keys and corresponding integer values that need to be averaged.
     *
     * @return
     */
    public Map<ValueKey, NumType> getDataMap();

}
