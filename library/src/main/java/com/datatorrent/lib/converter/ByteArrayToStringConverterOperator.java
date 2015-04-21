/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.converter;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import java.nio.charset.Charset;

public class ByteArrayToStringConverterOperator extends BaseOperator
{
  private Charset characterEncoding;

  public ByteArrayToStringConverterOperator()
  {
    characterEncoding = Charset.forName("UTF-8");
  }

  public Charset getCharacterEncoding()
  {
    return characterEncoding;
  }

  public void setCharacterEncoding(Charset characterEncoding)
  {
    this.characterEncoding = characterEncoding;
  }
  /**
   * Accepts byte arrays
   */
  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] message)
    {
      if (message != null) {
        outputString.emit(new String(message,characterEncoding));
      }
      else {
        outputString.emit(null);
      }
    }

  };

  /**
   * Output byte array converted to String
   */
  public final transient DefaultOutputPort<String> outputString = new DefaultOutputPort<String>();

}
