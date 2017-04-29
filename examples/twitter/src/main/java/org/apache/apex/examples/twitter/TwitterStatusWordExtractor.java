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
package org.apache.apex.examples.twitter;

import java.util.Arrays;
import java.util.HashSet;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * <p>TwitterStatusWordExtractor class.</p>
 *
 * @since 0.3.2
 */
public class TwitterStatusWordExtractor extends BaseOperator
{
  public HashSet<String> filterList;

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String text)
    {
      String[] strs = text.split(" ");
      if (strs != null) {
        for (String str : strs) {
          if (str != null && !filterList.contains(str) ) {
            output.emit(str);
          }
        }
      }
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    this.filterList = new HashSet<String>(Arrays.asList(new String[]{"", " ","I","you","the","a","to","as","he","him","his","her","she","me","can","for","of","and","or","but",
      "this","that","!",",",".",":","#","/","@","be","in","out","was","were","is","am","are","so","no","...","my","de","RT","on","que","la","i","your","it","have","with","?","when",
      "up","just","do","at","&","-","+","*","\\","y","n","like","se","en","te","el","I'm"}));
  }
}
