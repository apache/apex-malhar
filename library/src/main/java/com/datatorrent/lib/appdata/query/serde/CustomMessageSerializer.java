/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.query.serde;

import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;

/**
 * This interface defines a Message serializer, which will be used to convert a result to JSON. Classes
 * implementing this interface should have a public no-arg constructor.
 */
public interface CustomMessageSerializer
{
  /**
   * This message is called to serialize the given result to JSON.
   * @param message The data to serialize.
   * @param resultFormatter The formatter which will format primitive data in the result properly.
   * @return The JSON serialized result.
   */
  public abstract String serialize(Message message, ResultFormatter resultFormatter);
}
