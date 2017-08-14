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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperator;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.Stateless;

/**
 * WindowDataManager manages the state of an operator every application window. It can be used to replay tuples in
 * the input operator after re-deployment for a window which was not check-pointed but processing was completed before
 * failure.<br/>
 *
 * However, it cannot make any guarantees about the tuples emitted in the application window during which the operator
 * failed.<br/>
 *
 * The order of tuples is guaranteed for ordered input sources.
 *
 * <b>Important:</b> In order for an WindowDataManager to function correctly it cannot allow
 * checkpoints to occur within an application window and checkpoints must be aligned with
 * application window boundaries.
 *
 * @since 2.0.0
 */
public interface WindowDataManager extends Component<Context.OperatorContext>
{
  /**
   * Save the state for a window id.
   * @param object    state
   * @param windowId  window id
   * @throws IOException
   */
  void save(Object object, long windowId) throws IOException;

  /**
   * Gets the object saved for the provided window id. <br/>
   * Typically it is used to replay tuples of successive windows in input operators after failure.
   *
   * @param windowId window id
   * @return saved state for the window id.
   * @throws IOException
   */
  Object retrieve(long windowId) throws IOException;

  /**
   * Gets the largest window which was completed.
   * @return Returns the window id
   */
  long getLargestCompletedWindow();

  /**
   * Fetches the state saved for a window id for all the partitions.
   * <p/>
   * When an operator can partition itself dynamically then there is no guarantee that an input state which was being
   * handled by one instance previously will be handled by the same instance after partitioning. <br/>
   * For eg. An {@link AbstractFileInputOperator} instance which reads a File X till offset l (not check-pointed) may no
   * longer be the instance that handles file X after repartitioning as no. of instances may have changed and file X
   * is re-hashed to another instance. <br/>
   * The new instance wouldn't know from what point to read the File X unless it reads the idempotent storage of all the
   * operators for the window being replayed and fix it's state.
   *
   * @param windowId window id
   * @return saved state per operator partitions for the given window.
   * @throws IOException
   */
  Map<Integer, Object> retrieveAllPartitions(long windowId) throws IOException;

  /**
   * Delete the artifacts for windows <= windowId.
   *
   * @param windowId   window id
   * @throws IOException
   */
  void committed(long windowId) throws IOException;

  /**
   * Creates new window data managers during repartitioning.
   *
   * @param newCount count of new window data managers.
   * @param removedOperatorIds set of operator ids which were removed after partitioning.
   */
  List<WindowDataManager> partition(int newCount, Set<Integer> removedOperatorIds);

  /**
   * This {@link WindowDataManager} will never do recovery. This is a convenience class so that operators
   * can use the same logic for maintaining idempotency and avoiding idempotency.
   */
  class NoopWindowDataManager implements WindowDataManager
  {
    public long getLargestCompletedWindow()
    {
      return Stateless.WINDOW_ID;
    }

    @Override
    public Map<Integer, Object> retrieveAllPartitions(long windowId) throws IOException
    {
      return null;
    }

    @Override
    public List<WindowDataManager> partition(int newCount, Set<Integer> removedOperatorIds)
    {
      List<WindowDataManager> managers = new ArrayList<>();
      for (int i = 0; i < newCount; i++) {
        managers.add(new NoopWindowDataManager());
      }
      return managers;
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
    public void save(Object object, long windowId) throws IOException
    {
    }

    @Override
    public Object retrieve(long windowId) throws IOException
    {
      return null;
    }

    @Override
    public void committed(long windowId) throws IOException
    {
    }
  }
}
