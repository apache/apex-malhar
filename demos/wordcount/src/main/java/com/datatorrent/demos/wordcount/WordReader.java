/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.wordcount;

import java.util.regex.Pattern;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

// extracts words from input line
public class WordReader extends BaseOperator
{
  // default pattern for word-separators
  private static final Pattern nonWordDefault = Pattern.compile("[\\p{Punct}\\s]+");

  private String nonWordStr;    // configurable regex
  private transient Pattern nonWord;      // compiled regex

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

  public final transient DefaultInputPort<String>
    input = new DefaultInputPort<String>() {

    @Override
    public void process(String line)
    {
      // line; split it into words and emit them
      final String[] words = nonWord.split(line);
      for (String word : words) {
        if (word.isEmpty()) continue;
        output.emit(word);
      }
    }
  };

  public String getNonWordStr() {
    return nonWordStr;
  }

  public void setNonWordStr(String regex) {
    nonWordStr = regex;
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (null == nonWordStr) {
      nonWord = nonWordDefault;
    } else {
      nonWord = Pattern.compile(nonWordStr);
    }
  }

}
