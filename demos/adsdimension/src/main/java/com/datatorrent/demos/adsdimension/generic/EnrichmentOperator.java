/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.api.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.tools.javac.util.Assert;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * Enrichment
 * <p>
 * This class takes a HashMap tuple as input and extract the value of the lookupKey configured
 * for this operator. It then does a lookup in file to find matching entry and all key-value pairs
 * specified in the file is added to original tuple.
 *
 * The file contains data in json format, one entry per line. during setup entire file is read and
 * kept in memory for quick lookup.
 *
 * Example
 * If file contains following lines, and operator is configured with lookup key "productId"
 * { "productId": 1, "productCategory": 3 }
 * { "productId": 4, "productCategory": 10 }
 * { "productId": 3, "productCategory": 1 }
 *
 * And input tuple is
 * { amount=10.0, channelId=4, productId=3 }
 *
 * The tuple is modified as below before operator emits it on output port.
 * { amount=10.0, channelId=4, productId=3, productCategory=1 }
 * </p>
 */
public class EnrichmentOperator extends BaseOperator
{
  public transient DefaultOutputPort<Map<String, Object>> outputPort = new DefaultOutputPort<Map<String, Object>>();

  private transient static final ObjectMapper mapper = new ObjectMapper();
  private transient static final ObjectReader reader = mapper.reader(new TypeReference<Map<String,Object>>() { });
  private transient static final Logger logger = LoggerFactory.getLogger(EnrichmentOperator.class);

  /**
   * Location of the mapping file.
   */
  @NotNull
  private String filePath;

  /**
   * Check for new changes in file every scanInterval milliseconds.
   */
  private long scanInterval;

  /**
   * lookup key, index will be build maintained for value of lookup key for
   * quick searching.
   */
  @NotNull
  private String lookupKey;

  private String updateKeys;

  private transient long lastScanTimeStamp;

  private transient FileLoader loader;

  @VisibleForTesting
  protected transient Map<Object, Map<String, Object>> cache = Maps.newHashMap();
  private transient long lastKnownMtime;
  private transient List<String> keyList = Lists.newArrayList();

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  public long getScanInterval()
  {
    return scanInterval;
  }

  public void setScanInterval(long scanInterval)
  {
    this.scanInterval = scanInterval;
  }

  public String getLookupKey()
  {
    return lookupKey;
  }

  public void setLookupKey(String lookupKey)
  {
    this.lookupKey = lookupKey;
  }

  public String getUpdateKeys()
  {
    return updateKeys;
  }

  public void setUpdateKeys(String updateKeys)
  {
    this.updateKeys = updateKeys;
  }

  @Override public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    try {
      loader = new FileLoader(filePath);
      reloadData();
      lastScanTimeStamp = System.currentTimeMillis();

      if (updateKeys != null)
        keyList = Lists.newArrayList(updateKeys.split(","));

    } catch (IOException ex) {
      throw new RuntimeException("Failed to load mappings from the file.");
    }
  }

  @Override public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    /**
     * Check for modification of input.
     */
    long now  = System.currentTimeMillis();
    if ((now - lastScanTimeStamp) > scanInterval) {
      try {
        reloadData();
        lastScanTimeStamp = now;
      } catch (IOException ex) {
        throw new RuntimeException("Failed to load mappings from the file.");
      }
    }
  }

  /**
   * Reloads mapping data from file.
   */
  private void reloadData() throws IOException
  {
    /* Reload data from file, if it is modified after last scan */
    long mtime = loader.getModificationTime();
    if (mtime < lastKnownMtime)
      return;
    lastKnownMtime = mtime;

    FSDataInputStream in = loader.getInputStream();
    BufferedReader bin = new BufferedReader(new InputStreamReader(in));
    cache.clear();
    String line;
    while ((line = bin.readLine()) != null) {
      try {
        Map<String, Object> tuple = reader.readValue(line);
        updateLookupCache(tuple);
      } catch (JsonProcessingException parseExp) {
        logger.info("Unable to parse line {}", line);
      }
    }
    IOUtils.closeQuietly(bin);
    IOUtils.closeQuietly(in);
  }

  private void updateLookupCache(Map<String, Object> tuple)
  {
    if (tuple.containsKey(lookupKey)) {
      Object searchVal = tuple.get(lookupKey);
      cache.put(searchVal, tuple);
    }
  }

  public transient DefaultInputPort<Map<String, Object>> inputPort = new DefaultInputPort<Map<String, Object>>()
  {
    @Override public void process(Map<String, Object> tuple)
    {
      if (tuple.containsKey(lookupKey))
      {
        Object obj = tuple.get(lookupKey);
        Map<String, Object> extendedTuple = cache.get(obj);
        Map<String, Object> newAttributes = extendedTuple;
        if (keyList.size() != 0) {
           newAttributes = Maps.filterKeys(extendedTuple, Predicates.in(keyList));
        }
        tuple.putAll(newAttributes);
      }
      outputPort.emit(tuple);
    }
  };

  private static class FileLoader
  {
    private FileSystem fs;
    private Path filePath;

    private FileLoader(String file) throws IOException
    {
      Configuration conf = new Configuration();
      this.filePath = new Path(file);
      this.fs = FileSystem.newInstance(filePath.toUri(), conf);
      if (!fs.isFile(filePath))
        throw new IOException("Provided path " + file + " is not a file");
    }

    public long getModificationTime() throws IOException
    {
      FileStatus[] status = fs.listStatus(filePath);
      Assert.check(status.length == 1);
      return status[0].getModificationTime();
    }

    public FSDataInputStream getInputStream() throws IOException
    {
      return fs.open(filePath);
    }
  }
}
