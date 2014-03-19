/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datatorrent.lib.logs;

import java.util.Map;

/**
 * This interface is for extracting additional information given a value.
 * It could connect to a db to extract value, or using a 3rd party library to extract additional info.
 *
 * @since 0.9.4
 */
public interface InformationExtractor
{
  /**
   * Implementor should provide code that needs to be run for this extractor to be set up
   */
  public void setup();

  /**
   * Implementor should provide code that needs to be run for this extractor to be shut down
   */
  public void teardown();

  /**
   * Extracts information from the value and returns the extracted information as a map of string to object
   *
   * @param value
   * @return information map
   */
  public Map<String, Object> extractInformation(Object value);
}
