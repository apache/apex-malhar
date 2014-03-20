/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.db;

import java.io.IOException;

/**
 * This interface is for any object that needs to be connected to do operations
 *
 * @since 0.9.3
 */
public interface Connectable
{
  /**
   * Connects to the service.
   *
   * @throws IOException
   */
  public void connect() throws IOException;

  /**
   * Disconnects from the service.
   *
   * @throws IOException
   */
  public void disconnect() throws IOException;

  /**
   * Returns whether the service is connected.
   * 
   * @return
   */
  public boolean connected();
}
