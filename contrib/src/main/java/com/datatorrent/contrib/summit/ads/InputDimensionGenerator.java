/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.contrib.summit.ads;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 *
 */
public class InputDimensionGenerator extends BaseOperator
{

  private static final int dimSelect[][] = { {0, 0, 0}, {0, 0, 1}, {0, 1, 0}, {1, 0, 0}, {0, 1, 1}, {1, 0, 1}, {1, 1, 0}, {1, 1, 1} };
  private static final int dimSelLen = 8;

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<AdInfo>();

  @InputPortFieldAnnotation(name = "inputPort")
  public final transient DefaultInputPort<AdInfo> inputPort = new DefaultInputPort<AdInfo>() {

    @Override
    public void process(AdInfo tuple)
    {
      emitDimensions(tuple);
    }

  };

   private void emitDimensions(AdInfo adInfo) {
    for (int j = 0; j < dimSelLen; ++j) {
      AdInfo oadInfo = new AdInfo();
      if (dimSelect[j][0] == 1) oadInfo.setPublisherId(adInfo.getPublisherId());
      if (dimSelect[j][1] == 1) oadInfo.setAdvertiserId(adInfo.getAdvertiserId());
      if (dimSelect[j][2] == 1) oadInfo.setAdUnit(adInfo.getAdUnit());
      oadInfo.setClick(adInfo.isClick());
      oadInfo.setValue(adInfo.getValue());
      oadInfo.setTimestamp(adInfo.getTimestamp());
      this.outputPort.emit(oadInfo);
    }
  }


}
