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
package org.apache.apex.malhar.lib.window;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * The state that needs to be stored for each window. The state helps determine whether to throw away a window
 * (with allowed lateness in WindowOption), and whether to fire a trigger (with TriggerOption)
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class WindowState
{
  /**
   * The timestamp when the watermark arrives. If it has not arrived, -1.
   */
  public long watermarkArrivalTime = -1;

  /**
   * The timestamp when the last trigger was fired
   */
  public long lastTriggerFiredTime = -1;

  /**
   * The tuple count. Should be incremented for every tuple that belongs to the associated window
   */
  public long tupleCount = 0;

}
