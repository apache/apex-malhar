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
package org.apache.apex.malhar.lib.converter;

import java.nio.charset.Charset;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator converts Byte Array to String. User gets the option of providing character Encoding.
 *
 * @category Tuple Converters
 * @tags byte, string
 *
 * @since 2.1.0
 */
public class ByteArrayToStringConverterOperator extends BaseOperator
{
  private Charset characterEncoding;

  public String getCharacterEncoding()
  {
    return characterEncoding.name();
  }

  public void setCharacterEncoding(String characterEncoding)
  {
    this.characterEncoding = Charset.forName(characterEncoding);
  }



  /**
   * Input port which accepts byte array.
   */
  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] message)
    {
      output.emit(characterEncoding == null ? new String(message) : new String(message, characterEncoding));
    }

  };

  /**
   * Output port which outputs String converted from byte array.
   */
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

}
