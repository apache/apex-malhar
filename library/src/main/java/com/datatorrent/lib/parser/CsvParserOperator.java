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
package com.datatorrent.lib.parser;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.ShipContainingJars;

/**
 * <p>A general CSV parser to parse csv String/bytearray content to Map</p><br>
 *
 * Properties:<br>
 * <b>quote</b>: Quote mark for csv file<br>
 * <b>separator</b>: Separator mark for csv file<br>
 * <br>
 *
 * Shipped jars with this operator:<br>
 *  <b>org.supercsv.io.CsvMapReader<br>
 *
 *  <br>
 *
 * @since 0.9.2
 */

@ShipContainingJars(classes=CsvMapReader.class)
public class CsvParserOperator extends BaseOperator
{
  
  private static final Logger logger = LoggerFactory.getLogger(CsvParserOperator.class);
  
  private char quote = '"';
  
  private char separator = ',';
  
  private Charset charset = Charset.defaultCharset();
  
  @NotNull
  private transient CSVHeaderMapping headerMapping = null;
  
  public transient DefaultInputPort<String> stringInput = new DefaultInputPort<String>(){

    @Override
    public void process(String tuple)
    {      
      processTuple(tuple);
    }
    
  };
  
  public transient DefaultInputPort<byte[]> byteArrayInput = new DefaultInputPort<byte[]>() {

    @Override
    public void process(byte[] tuple)
    {
      processTuple(tuple);
    }
  };
  
  public transient DefaultOutputPort<Map<String, Object>> mapOutput = new DefaultOutputPort<Map<String,Object>>();

  @Override
  public void beginWindow(long windowId)
  {
  }

  protected void processTuple(String tuple)
  {
    StringReader reader = new StringReader(tuple);
    ICsvMapReader mapReader = new CsvMapReader(reader, new CsvPreference.Builder(quote, separator, CsvPreference.STANDARD_PREFERENCE.getEndOfLineSymbols()).build());
    try {
      String[] header = headerMapping.getHeaders();
      if (header == null) {
        header = mapReader.getHeader(true);
      }
      Map<String, Object> customerMap;
      while ((customerMap = mapReader.read(header, headerMapping.getProcessors())) != null) {
        mapOutput.emit(customerMap);
      }
    } catch (Exception e) {
      logger.error("Parsing csv error", e);
    } finally{
      try {
        mapReader.close();
      } catch (IOException e) {
        logger.error("Parsing csv error", e);
      }
    }
  }
  
  protected void processTuple(byte[] tuple)
  {
    processTuple(new String(tuple, charset));
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {

  }

  public char getQuote()
  {
    return quote;
  }

  public void setQuote(char quote)
  {
    this.quote = quote;
  }

  public char getSeparator()
  {
    return separator;
  }

  public void setSeparator(char separator)
  {
    this.separator = separator;
  }

  public CSVHeaderMapping getHeaderMapping()
  {
    return headerMapping;
  }
  
  public void setHeaderMapping(CSVHeaderMapping headerMapping)
  {
    this.headerMapping = headerMapping;
  }
  
  public void setCharset(Charset charset)
  {
    this.charset = charset;
  }
  
  public Charset getCharset()
  {
    return charset;
  }

}
