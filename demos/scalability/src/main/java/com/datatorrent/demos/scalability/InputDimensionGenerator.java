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
package com.datatorrent.demos.scalability;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * <p>InputDimensionGenerator class.</p>
 *
 * @since 0.3.2
 */
public class InputDimensionGenerator extends BaseOperator
{

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
    for (int i =0; i < 8; ++i) {
      AdInfo eadInfo = new AdInfo();
      if ((i & 1) != 0) eadInfo.setPublisherId(adInfo.getPublisherId());
      if ((i & 2) != 0) eadInfo.setAdvertiserId(adInfo.getAdvertiserId());
      if ((i & 4) != 0) eadInfo.setAdUnit(adInfo.getAdUnit());
      eadInfo.setClick(adInfo.isClick());
      eadInfo.setValue(adInfo.getValue());
      eadInfo.setTimestamp(adInfo.getTimestamp());
      this.outputPort.emit(eadInfo);
    }
  }


}
