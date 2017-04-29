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
package org.apache.apex.malhar.lib.state.spillable;

import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.state.managed.BucketProvider;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;

/**
 * Implementations of this interface are used by Spillable datastructures to spill data to disk.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface SpillableStateStore extends BucketedState, Component<Context.OperatorContext>,
    Operator.CheckpointNotificationListener, WindowListener, BucketProvider
{
}
