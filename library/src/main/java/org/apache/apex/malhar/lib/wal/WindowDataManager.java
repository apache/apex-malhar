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
package org.apache.apex.malhar.lib.wal;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

/**
 * An idempotent storage manager allows an operator to emit the same tuples in every replayed application window.
 * An idempotent agent cannot make any guarantees about the tuples emitted in the application window which fails.
 *
 * The order of tuples is guaranteed for ordered input sources.
 *
 * <b>Important:</b> In order for an idempotent storage manager to function correctly it cannot allow
 * checkpoints to occur within an application window and checkpoints must be aligned with
 * application window boundaries.
 *
 * @since 2.0.0
 */
public interface WindowDataManager extends StorageAgent, Component<Context.OperatorContext>
{
  /**
   * Gets the largest window for which there is recovery data.
   * @return Returns the window id
   */
  long getLargestRecoveryWindow();

  /**
   * When an operator can partition itself dynamically then there is no guarantee that an input state which was being
   * handled by one instance previously will be handled by the same instance after partitioning. <br/>
   * For eg. An {@link AbstractFileInputOperator} instance which reads a File X till offset l (not check-pointed) may no
   * longer be the instance that handles file X after repartitioning as no. of instances may have changed and file X
   * is re-hashed to another instance. <br/>
   * The new instance wouldn't know from what point to read the File X unless it reads the idempotent storage of all the
   * operators for the window being replayed and fix it's state.
   *
   * @param windowId window id.
   * @return mapping of operator id to the corresponding state
   * @throws IOException
   */
  Map<Integer, Object> load(long windowId) throws IOException;

  /**
   * Delete the artifacts of the operator for windows <= windowId.
   *
   * @param operatorId operator id
   * @param windowId   window id
   * @throws IOException
   */
  void deleteUpTo(int operatorId, long windowId) throws IOException;

  /**
   * This informs the idempotent storage manager that operator is partitioned so that it can set properties and
   * distribute state.
   *
   * @param newManagers        all the new idempotent storage managers.
   * @param removedOperatorIds set of operator ids which were removed after partitioning.
   */
  void partitioned(Collection<WindowDataManager> newManagers, Set<Integer> removedOperatorIds);

  /**
   * Returns an array of windowIds for which data was stored by atleast one partition. The array
   * of winodwIds is sorted.
   *
   * @return An array of windowIds for which data was stored by atleast one partition. The array
   * of winodwIds is sorted.
   * @throws IOException
   */
  long[] getWindowIds() throws IOException;

  /**
   * This {@link WindowDataManager} will never do recovery. This is a convenience class so that operators
   * can use the same logic for maintaining idempotency and avoiding idempotency.
   */
  class NoopWindowDataManager implements WindowDataManager
  {
    @Override
    public long getLargestRecoveryWindow()
    {
      return Stateless.WINDOW_ID;
    }

    @Override
    public Map<Integer, Object> load(long windowId) throws IOException
    {
      return null;
    }

    @Override
    public void partitioned(Collection<WindowDataManager> newManagers, Set<Integer> removedOperatorIds)
    {
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void save(Object object, int operatorId, long windowId) throws IOException
    {
    }

    @Override
    public Object load(int operatorId, long windowId) throws IOException
    {
      return null;
    }

    @Override
    public void delete(int operatorId, long windowId) throws IOException
    {
    }

    @Override
    public void deleteUpTo(int operatorId, long windowId) throws IOException
    {
    }

    @Override
    public long[] getWindowIds(int operatorId) throws IOException
    {
      return new long[0];
    }

    @Override
    public long[] getWindowIds() throws IOException
    {
      return new long[0];
    }
  }
}
