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
import java.io.Reader;
import java.io.Serializable;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.ShipContainingJars;

/**
 * <p>
 * A general CSV parser to parse csv String/bytearray content to Map
 * </p>
 * <br>
 *
 * Properties:<br>
 * <b>quote</b>: Quote mark for csv file<br>
 * <b>separator</b>: Separator mark for csv file<br>
 * <br>
 *
 * Shipped jars with this operator:<br>
 * <b>org.supercsv.io.CsvMapReader<br>
 *
 * <br>
 *
 * @since 0.9.2
 */

@ShipContainingJars(classes = CsvMapReader.class)
public class CsvParserOperator extends BaseOperator
{

  private static final Logger logger = LoggerFactory.getLogger(CsvParserOperator.class);

  private char quote = '"';

  private char separator = ',';

    @NotNull
  private CSVHeaderMapping headerMapping = null;

  @NotNull
  private transient ReusableStringReader csvStringReader = new ReusableStringReader();

  private transient ICsvMapReader mapReader;

  private transient String[] headers = null;

  private transient CellProcessor[] columnProcessors = null;

  public transient DefaultInputPort<String> stringInput = new DefaultInputPort<String>() {

    @Override
    public void process(String tuple)
    {
      processTuple(tuple);
    }

  };

  @InputPortFieldAnnotation (optional = true, name = "byteInput")
  public transient DefaultInputPort<byte[]> byteArrayInput = new DefaultInputPort<byte[]>() {

    @Override
    public void process(byte[] tuple)
    {
      processTuple(tuple);
    }
  };

  public transient DefaultOutputPort<Map<String, Object>> mapOutput = new DefaultOutputPort<Map<String, Object>>();

  @Override
  public void beginWindow(long windowId)
  {
  }

  protected void processTuple(String tuple)
  {
    try {

      if (headers == null) {
        headers = mapReader.getHeader(true);
      }
      Map<String, Object> customerMap;
      csvStringReader.open(tuple);
      while ((customerMap = mapReader.read(headers, columnProcessors)) != null) {
        mapOutput.emit(customerMap);
      }
    } catch (Exception e) {
      logger.error("Parsing csv error", e);
    }
  }

  protected void processTuple(byte[] tuple)
  {
    processTuple(new String(tuple));
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (!(headerMapping instanceof Serializable)) {
      throw new IllegalArgumentException("Header mapping should be serializable!");
    }
    headers = headerMapping.getHeaders();
    columnProcessors = headerMapping.getProcessors();

    mapReader = new CsvMapReader(csvStringReader, new CsvPreference.Builder(quote, separator, CsvPreference.STANDARD_PREFERENCE.getEndOfLineSymbols()).surroundingSpacesNeedQuotes(true).build());
  }

  @Override
  public void teardown()
  {
    try {
      mapReader.close();
    } catch (IOException e) {
      logger.error("Parsing csv error", e);
    }
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

  public static class ReusableStringReader extends Reader
  {
    private String str;
    private int length;
    private int next = 0;

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException
    {
      ensureOpen();
      if ((off < 0) || (off > cbuf.length) || (len < 0) || ((off + len) > cbuf.length) || ((off + len) < 0)) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      if (next >= length)
        return -1;
      int n = Math.min(length - next, len);
      str.getChars(next, next + n, cbuf, off);
      next += n;
      return n;
    }

    /**
     * Reads a single character.
     *
     * @return The character read, or -1 if the end of the stream has been reached
     *
     * @exception IOException
     *              If an I/O error occurs
     */
    public int read() throws IOException
    {
      ensureOpen();
      if (next >= length)
        return -1;
      return str.charAt(next++);
    }

    public boolean ready() throws IOException
    {
      ensureOpen();
      return true;
    }

    @Override
    public void close() throws IOException
    {
      str = null;
    }

    /** Check to make sure that the stream has not been closed */
    private void ensureOpen() throws IOException
    {
      if (str == null) {
        throw new IOException("Stream closed");
      }
    }

    public void open(String str) throws IOException
    {
      this.str = str;
      this.length = this.str.length();
      this.next = 0;
    }

  }

}
