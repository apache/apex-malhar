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
package com.datatorrent.apps.logstream.PropertyRegistry;

/**
 * Interface for property registry which can be implemented and shared across the dag
 * Properties can be added to the registry during dag setup and then be accessible in all
 * interested operators to look them up.
 * @param <T>
 */
public interface PropertyRegistry<T>
{
  /**
   * binds a given name and value, assigns a unique index to the name value pair and returns it
   * @param name
   * @param value
   * @return
   */
  public int bind(T name, T value);

  /**
   * Returns the index of name value pair if exists, else returns -1
   * @param name
   * @param value
   * @return
   */
  public int getIndex(T name, T value);

  /**
   * Returns the name associated with given index
   * @param register
   * @return
   */
  public T lookupName(int register);

  /**
   * Returns the value associated with given index
   * @param register
   * @return
   */
  public T lookupValue(int register);

  /**
   * Returns a list of values associated with given name
   * @param name
   * @return
   */
  public T[] list(String name);
}
