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

/**
 * This interface defines a validator which validates the state of deserialized messages. Classes
 * implementing this interface should have a public no-arg constructor.
 */
public interface CustomMessageValidator
{
  /**
   * Validates the state of the given deserialized message.
   * @param query The deserialized message.
   * @param context Any contextual information required to properly validate the given message.
   * @return True if the state of the deserialized message is valid.
   */
  public boolean validate(Message query, Object context);
}
