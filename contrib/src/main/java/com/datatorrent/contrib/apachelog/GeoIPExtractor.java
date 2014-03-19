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

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import javax.validation.constraints.NotNull;

import com.datatorrent.lib.logs.InformationExtractor;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This extractor uses maxmind API to extract Geo information from an IP address
 *
 * @since 0.9.4
 */
public class GeoIPExtractor implements InformationExtractor
{
  private static final Logger LOG = LoggerFactory.getLogger(GeoIPExtractor.class);
  private transient LookupService reader;
  /**
   * The local path that contains the maxmind "legacy" GeoIP db
   */
  @NotNull
  private String databasePath;

  public String getDatabasePath()
  {
    return databasePath;
  }

  public void setDatabasePath(String databasePath)
  {
    this.databasePath = databasePath;
  }

  @Override
  public void setup()
  {
    try {
      reader = new LookupService(databasePath, LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    reader.close();
  }

  @Override
  public Map<String, Object> extractInformation(Object value)
  {
    Map<String, Object> m = new HashMap<String, Object>();
    try {
      Location location = reader.getLocation(value.toString());
      if (location != null) {
        m.put("ipCountry", location.countryCode);
        m.put("ipRegion", location.region);
        m.put("ipCity", location.city);
      }
    }
    catch (Exception ex) {
      LOG.error("Caught exception when looking up Geo IP for {}:", value, ex);
    }
    return m;
  }

}
