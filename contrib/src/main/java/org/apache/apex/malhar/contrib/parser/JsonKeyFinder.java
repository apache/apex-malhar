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
package org.apache.apex.malhar.contrib.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.json.simple.parser.ContentHandler;

import com.google.protobuf.TextFormat.ParseException;

/**
 * A concrete implementation of Json ContentHandler<br>
 * Matches JSON keys set from the {@link StreamingJsonParser }
 *
 * @since 3.5.0
 */
public class JsonKeyFinder implements ContentHandler
{
  public JsonKeyFinder()
  {
    keyValMap = new HashMap<>();
  }

  public int getKeyCount()
  {
    return keyCount;
  }

  public void setKeyCount(int keyCount)
  {
    this.keyCount = keyCount;
  }

  private Object value;
  private HashMap<Object, Object> keyValMap;
  private int keyCount = 0;

  public HashMap<Object, Object> getKeyValMap()
  {
    return keyValMap;
  }

  public void setKeyValMap(HashMap<Object, Object> keyValMap)
  {
    this.keyValMap = keyValMap;
  }

  private boolean found = false;
  private boolean end = false;
  private String key;

  private ArrayList<String> matchKeyList;

  public void setMatchKeyList(ArrayList<String> matchKeyList)
  {
    this.matchKeyList = matchKeyList;
  }

  public ArrayList<String> getMatchKeyList()
  {
    return matchKeyList;
  }

  public Object getValue()
  {
    return value;
  }

  public boolean isEnd()
  {
    return end;
  }

  public void setFound(boolean found)
  {
    this.found = found;
  }

  public boolean isFound()
  {
    return found;
  }

  public void startJSON() throws ParseException, IOException
  {
    found = false;
    end = false;
  }

  public void endJSON() throws ParseException, IOException
  {
    end = true;
  }

  public boolean primitive(Object value) throws ParseException, IOException
  {
    if (getMatchKeyList().contains(key)) {
      found = true;
      this.value = value;
      keyValMap.put(key, value);
      key = null;
      keyCount++;
      return false;
    }
    return true;
  }

  public boolean startArray() throws ParseException, IOException
  {
    return true;
  }

  public boolean startObject() throws ParseException, IOException
  {
    return true;
  }

  public boolean startObjectEntry(String key) throws ParseException, IOException
  {
    this.key = key;
    return true;
  }

  public boolean endArray() throws ParseException, IOException
  {
    return false;
  }

  public boolean endObject() throws ParseException, IOException
  {
    return true;
  }

  public boolean endObjectEntry() throws ParseException, IOException
  {
    return true;
  }
}
