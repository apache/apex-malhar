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
package com.datatorrent.contrib.apachelog;

import java.util.Map;

import com.datatorrent.lib.logs.InformationExtractor;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This extractor extracts the time stamp in milliseconds from epoch from an arbitrary date string
 *
 * @since 0.9.4
 */
public class TimestampExtractor implements InformationExtractor
{
  private static final Logger LOG = LoggerFactory.getLogger(GeoIPExtractor.class);
  @NotNull
  private String dateFormatString;
  private transient DateFormat dateFormat;  // date format is not serializable

  public void setDateFormatString(String dateFormatString)
  {
    this.dateFormatString = dateFormatString;
  }

  @Override
  public void setup()
  {
    dateFormat = new SimpleDateFormat(dateFormatString);
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public Map<String, Object> extractInformation(Object value)
  {
    Map<String, Object> m = new HashMap<String, Object>();
    try {
      Date date = dateFormat.parse((String)value);
      m.put("timestamp", date.getTime());
    }
    catch (ParseException ex) {
      LOG.error("Error parsing \"{}\" to timestamp using \"{}\":", value, dateFormatString, ex);
    }
    return m;
  }

}
