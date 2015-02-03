/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.demos.dimensions.ads.InputItemGenerator;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Generates AdInfo events and sends them out in JSON format.
 * <p>
 * By default 10000 events are generated in every window.  See {@link com.datatorrent.demos.dimensions.ads.InputItemGenerator}
 * for additional details on tunable settings settings like blastCount, numPublishers, numAdvertisers, numAdUnits, etc.
 *
 * @displayName JSON AdInfo Generator
 * @category Input
 * @tags input, generator
 *
 * @since 2.0.0
 */
public class JsonAdInfoGenerator extends InputItemGenerator
{
  public final transient DefaultOutputPort<byte[]> jsonOutput = new DefaultOutputPort<byte[]>();
  private static final ObjectMapper mapper = new ObjectMapper();

  // Limit number of emitted tuples per window
  private long maxTuplesPerWindow = 40000;
  private long currentTuplesInWindow = 0;

  public long getMaxTuplesPerWindow() {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(long maxTuplesPerWindow) {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentTuplesInWindow = 0;
    super.beginWindow(windowId);
  }

  @Override
  public void emitTuple(AdInfo adInfo)
  {
    if (currentTuplesInWindow++ <= maxTuplesPerWindow) {
      try {
        this.jsonOutput.emit(mapper.writeValueAsBytes(adInfo));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
