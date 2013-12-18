/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.telecom.util;

import java.io.Serializable;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ift.CellProcessor;

import com.datatorrent.lib.parser.CSVHeaderMapping;

/**
 *  @since 0.9.2
 * @author gaurav gupta <gaurav@datatorrent.com>
 *
 */
public class TestHeaderMapping implements CSVHeaderMapping, Serializable
{
  private String[] header;

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public CellProcessor[] getProcessors()
  {
      CellProcessor[] processor = new CellProcessor[header.length];
      for (CellProcessor cellProcessor : processor) {
        cellProcessor = new Optional();
      }
      return processor;
  }

  @Override
  public String[] getHeaders()
  {
    return header;
  }
  
  public void setHeaders(String[] headers){
    this.header = headers;
  }
}
