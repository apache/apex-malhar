/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.contrib.parser.AbstractCsvParser;
import com.datatorrent.common.util.DTThrowable;
import java.io.IOException;
import java.util.Map;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

/*
 * An implementation of AbstractParser which emits a map.
 * The output is a map with key being the field name supplied by user and value being the value of that field
 * from input stream.
 */
public class CsvToMapParser extends AbstractCsvParser<Map<String, Object>>
{
  protected transient ICsvMapReader csvReader = null;

  /**
   * This method creates an instance of csvMapReader.
   * @param reader
   * @param preference
   * @return CSV Map Reader
   */
  @Override
  public ICsvMapReader getReader(ReusableStringReader reader, CsvPreference preference)
  {
    csvReader = new CsvMapReader(reader, preference);
    return csvReader;
  }

  /**
   * This method reads input stream data values into a map.
   * @return Map containing key as field name given by user and value of the field.
   */
  @Override
  public Map<String, Object> readData(String[] properties, CellProcessor[] processors)
  {
    Map<String, Object> fieldValueMapping = null;
    try {
      fieldValueMapping = csvReader.read(properties, processors);
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    return fieldValueMapping;

  }

}
