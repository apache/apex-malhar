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
package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;

/**
 * This class represents the most basic structure of an AppData message. At the core
 * of every AppData message there is the following json construct:<br/>
 * <pre>
 * {@code
 * {
 *  "type":"MY_MESSAGE_TYPE"
 *  ...
 * }
 * }
 * </pre>
 * <br/>
 * <br/>
 * As can be seen above, the common element of an AppData message is a message type, which is what
 * this class encapsulates.
 */
public abstract class Message
{
  /**
   * JSON key string for message type.
   */
  public static final String FIELD_TYPE = "type";

  /**
   * The type of the Message.
   */
  private String type;

  /**
   * Creates a message.
   */
  public Message()
  {
  }

  /**
   * Creates a message an sets it's type to be the specified type.
   * @param type The type of the message.
   */
  public Message(String type)
  {
    Preconditions.checkNotNull(type);
    this.type = type;
  }

  /**
   * Returns the type of the message.
   * @return The type of the message.
   */
  public String getType()
  {
    return type;
  }

  /**
   * Sets the type of the message.
   * @param type The type of the message.
   */
  public void setType(String type)
  {
    this.type = type;
  }

}
