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
package org.apache.apex.malhar.lib.math;

import java.io.StringReader;
import java.util.List;

import org.xml.sax.InputSource;

import com.datatorrent.api.DefaultOutputPort;

/**
 * An implementation of the AbstractXmlKeyValueCartesianProduct operator that takes in the xml document
 * as a String input and outputs the cartesian product as Strings.
 *
 * @displayName Xml Key Value String Cartesian Product
 * @category Math
 * @tags cartesian product, string, xml
 * @since 1.0.1
 */
public class XmlKeyValueStringCartesianProduct extends AbstractXmlKeyValueCartesianProduct<String>
{

  InputSource source = new InputSource();

  /**
   * Output port that emits cartesian product as Strings.
   */
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  protected InputSource getInputSource(String tuple)
  {
    source.setCharacterStream(new StringReader(tuple));
    return source;
  }

  @Override
  protected void processResult(List<String> result, String tuple)
  {
    for (String str : result) {
      output.emit(str);
    }
  }
}
