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
package org.apache.apex.malhar.lib.window.accumulation;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.commons.lang3.tuple.MutablePair;

/**
 * Average Accumulation
 *
 * @since 3.5.0
 */
public class Average implements Accumulation<Double, MutablePair<Double, Long>, Double>
{
  @Override
  public MutablePair<Double, Long> defaultAccumulatedValue()
  {
    return new MutablePair<>(0.0, 0L);
  }

  @Override
  public MutablePair<Double, Long> accumulate(MutablePair<Double, Long> accu, Double input)
  {
    accu.setLeft(accu.getLeft() * ((double)accu.getRight() / (accu.getRight() + 1)) + input / (accu.getRight() + 1));
    accu.setRight(accu.getRight() + 1);
    return accu;
  }

  @Override
  public MutablePair<Double, Long> merge(MutablePair<Double, Long> accu1, MutablePair<Double, Long> accu2)
  {
    accu1.setLeft(accu1.getLeft() * ((double)accu1.getRight() / accu1.getRight() + accu2.getRight()) +
        accu2.getLeft() * ((double)accu2.getRight() / accu1.getRight() + accu2.getRight()));
    accu1.setRight(accu1.getRight() + accu2.getRight());
    return accu1;
  }

  @Override
  public Double getOutput(MutablePair<Double, Long> accumulatedValue)
  {
    return accumulatedValue.getLeft();
  }

  @Override
  public Double getRetraction(Double value)
  {
    // TODO: Need to add implementation for retraction.
    return 0.0;
  }
}
