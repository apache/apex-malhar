/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.stream.api.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.stream.api.Option.WatermarkGenerator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * A Post-Process operator that can be attached to direct upstream operator to generate {@link ControlTuple.Watermark}
 */
public class PostprocessOperator<T> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(PostprocessOperator.class);


  public final transient DefaultOutputPort<ControlTuple> controlOutput = new DefaultOutputPort<>();

  public final transient DefaultInputPort<T> dataInput = new DefaultInputPort<T>()
  {
    @Override
    public void process(T o)
    {
      if (watermarkGenerator != null) {
        ControlTuple.Watermark watermark = watermarkGenerator.getWatermarkFromTuple(o);
        if (watermark != null) {
          controlOutput.emit(watermark);
        }
      }
      // data is just passing through
      dataOutput.emit(o);
    }
  };

  public final transient DefaultInputPort<ControlTuple> controlInput = new DefaultInputPort<ControlTuple>()
  {
    @Override
    public void process(ControlTuple controlTuple)
    {
      controlOutput.emit(controlTuple);
    }
  };

  public final transient DefaultOutputPort<T> dataOutput = new DefaultOutputPort<>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (watermarkGenerator != null) {
      watermarkGenerator.setup(context);
    }
  }

  @Override
  public void teardown()
  {
    if (watermarkGenerator != null) {
      watermarkGenerator.teardown();
    }
  }

  @Override
  public void beginWindow(long l)
  {
  }

  @Override
  public void endWindow()
  {
    if (watermarkGenerator != null) {
      ControlTuple.Watermark w = watermarkGenerator.currentWatermark();
      if (w != null) {
        logger.debug("Emitting watermark {}", w);
        controlOutput.emit(w);
      }
    }
  }

  private WatermarkGenerator watermarkGenerator;

  public void setWatermarkGenerator(WatermarkGenerator watermarkGenerator)
  {
    this.watermarkGenerator = watermarkGenerator;
  }

  public WatermarkGenerator getWatermarkGenerator()
  {
    return watermarkGenerator;
  }

}
