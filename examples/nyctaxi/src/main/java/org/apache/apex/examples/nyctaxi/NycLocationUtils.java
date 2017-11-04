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
package org.apache.apex.examples.nyctaxi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Throwables;

/**
 * Provides utilities for zip codes and lat-lon coordinates in New York City.
 *
 * @since 3.8.0
 */
public class NycLocationUtils
{
  public static class ZipRecord
  {
    public final String zip;
    public final double lat;
    public final double lon;
    public String[] neighboringZips;

    public ZipRecord(String zip, double lat, double lon)
    {
      this.zip = zip;
      this.lat = lat;
      this.lon = lon;
    }
  }

  private static Map<String, ZipRecord> zipRecords = new HashMap<>();

  static {
    // setup of NYC zip data.
    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(NycLocationUtils.class.getResourceAsStream("/nyc_zip_codes.csv")))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] s = line.split(",");
        String zip = s[0].trim();
        double lat = Double.valueOf(s[1].trim());
        double lon = Double.valueOf(s[2].trim());
        zipRecords.put(zip, new ZipRecord(zip, lat, lon));
      }
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
    for (Map.Entry<String, ZipRecord> entry : zipRecords.entrySet()) {
      final ZipRecord entryValue = entry.getValue();
      List<String> zips = new ArrayList<>(zipRecords.keySet());

      Collections.sort(zips, new Comparator<String>()
      {
        @Override
        public int compare(String s1, String s2)
        {
          ZipRecord z1 = zipRecords.get(s1);
          ZipRecord z2 = zipRecords.get(s2);
          double dist1 = Math.pow(z1.lat - entryValue.lat, 2) + Math.pow(z1.lon - entryValue.lon, 2);
          double dist2 = Math.pow(z2.lat - entryValue.lat, 2) + Math.pow(z2.lon - entryValue.lon, 2);
          return Double.compare(dist1, dist2);
        }
      });
      entryValue.neighboringZips = zips.subList(0, 8).toArray(new String[]{});
    }
  }

  public static String getZip(double lat, double lon)
  {
    // Brute force to get the nearest zip centoid. Should be able to optimize this.
    double minDist = Double.MAX_VALUE;
    String zip = null;
    for (Map.Entry<String, ZipRecord> entry : zipRecords.entrySet()) {
      ZipRecord zipRecord = entry.getValue();
      double dist = Math.pow(zipRecord.lat - lat, 2) + Math.pow(zipRecord.lon - lon, 2);
      if (dist < minDist) {
        zip = entry.getKey();
        minDist = dist;
      }
    }
    return zip;
  }

  public static String[] getNeighboringZips(String zip)
  {
    ZipRecord zipRecord = zipRecords.get(zip);
    if (zipRecord != null) {
      return zipRecord.neighboringZips;
    } else {
      return null;
    }
  }
}
