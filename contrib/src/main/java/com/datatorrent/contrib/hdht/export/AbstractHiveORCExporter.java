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

package com.datatorrent.contrib.hdht.export;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.beust.jcommander.internal.Sets;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.HDHTCodec;
import com.datatorrent.contrib.hds.HDSFileAccess;
import com.datatorrent.contrib.hds.HDSFileAccess.HDSFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.sql.*;


/**
 * Exports HDHT files into ORC format.
 */
public abstract class AbstractHiveORCExporter<EVENT> extends AbstractHDHTFileExporter<EVENT> {

  AbstractHiveORCExporter(HDHTCodec<EVENT> codec) {
    super(codec);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHiveORCExporter.class);

  private long stripeSize = 100000;
  private int bufferSize = 10000;
  private transient ObjectInspector inspector;
  private String hiveTable;
  private List<String> hivePartitions = Lists.newArrayList();
  private String tmpBasePath = getBasePath() + ".tmp";
  private FileSystem fs;

  private static String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
  private static String hiveURL = "jdbc:hive2://localhost:10000/default";
  private static String hiveUser = "";
  private static String hivePassword = "";


  protected FileSystem getFileSystem() {
    if (fs == null) {
      Path dataFilePath = new Path(getBasePath());
      try {
        fs = FileSystem.newInstance(dataFilePath.toUri(), new Configuration());
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
    return fs;
  }

  public static String getHiveDriverName() {
    return hiveDriverName;
  }

  public static void setHiveDriverName(String hiveDriverName) {
    AbstractHiveORCExporter.hiveDriverName = hiveDriverName;
  }

  public static String getHiveURL() {
    return hiveURL;
  }

  public static void setHiveURL(String hiveURL) {
    AbstractHiveORCExporter.hiveURL = hiveURL;
  }

  public static String getHiveUser() {
    return hiveUser;
  }

  public static void setHiveUser(String hiveUser) {
    AbstractHiveORCExporter.hiveUser = hiveUser;
  }

  public static String getHivePassword() {
    return hivePassword;
  }

  public static void setHivePassword(String hivePassword) {
    AbstractHiveORCExporter.hivePassword = hivePassword;
  }

  public String getHiveTable() {
    return hiveTable;
  }

  public void setHiveTable(String hiveTable) {
    this.hiveTable = hiveTable;
  }

  public String getTmpBasePath() {
    return tmpBasePath;
  }

  public void setTmpBasePath(String tmpBasePath) {
    this.tmpBasePath = tmpBasePath;
  }


  public long getStripeSize() {
    return stripeSize;
  }

  public void setStripeSize(long stripeSize) {
    this.stripeSize = stripeSize;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  /**
   * In Hive partitions can be altered with following statement
   *
   * ALTER TABLE sales
   * PARTITION (country = 'US', year = 2014, month = 12, day = 4)
   * SET LOCATION = 'sales/partitions/us/2014/12/4' ;
   *
   * This swaps Hive partition to new data directory, but does not remove old data from DFS
   *
   */
  //TODO - should accept a set and process them all in batches - do not open individual connections
  private void updateHivePartitionLocations() {
    Connection con = null;
    try {
      Class.forName(getHiveDriverName());
      con = DriverManager.getConnection(getHiveURL(), getHiveUser(), getHivePassword());
      Statement stmt = null;
      //TODO - get partition contents and location based on info supplied as method args
      String sql = "ALTER TABLE " + getHiveTable() + " PARTITION ( ... )  SET LOCATION =  ' ...  '";
      try {
        stmt = con.createStatement();
        stmt.execute(sql);
      }
      catch (SQLException ex) {
        LOG.info("Failed to execute: {}", sql, ex);
        DTThrowable.rethrow(ex);
      } finally {
        if (stmt != null) stmt.close();
      }
      con.close();
    }
    catch (ClassNotFoundException ce) {
      LOG.error("Failed to load Hive driver: {}", getHiveDriverName(), ce);
      DTThrowable.rethrow(ce);
    }
    catch (SQLException ex) {
      LOG.error("Failed to connect to Hive url: '{}' user: '{}'", getHiveURL(), getHiveUser(), ex);
      DTThrowable.rethrow(ex);
    }
  }


  /**
   * Provides ObjectInspector necessary to instantiate ORC Writer.
   * @return ObjectInspector used to convert objects into ORC rows.
   */
  abstract ObjectInspector createObjectInspector();
//      synchronized (AbstractHiveORCFileExporter.class) {
//        inspector = ObjectInspectorFactory.getReflectionObjectInspector(eventClass, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
//      }

  public ObjectInspector getInspector() {
    if (inspector == null) {
      inspector = createObjectInspector();
    }
    return inspector;
  }

  /**
   * This method is designed to help place ORC files into proper Hive partition structure.
   *
   * In Hive partitions can be defined as: PARTITIONED BY (country STRING, year INT, month INT, day INT)
   * Resulting in files being placed into: country=US/year=2014/month=12/day=4
   *
   * This method provides the opportunity to place exported ORC files into similar path structures
   * based on the fields of the provided record.
   *
   * @param record
   * @return
   */
  abstract Path getHivePartitionPath(EVENT record);
//  {
//    Path path = new Path("");
//    for (String partition: hivePartitions) {
//      //TODO - build partition path based on record object
//    }
//    return path;
//  }

  /**
   * Return ORC Writer object based in a given path.
   */
  private Writer getORCWriter(Path path) throws IOException {
    return OrcFile.createWriter(path,
            OrcFile.writerOptions(new Configuration())
                    .inspector(getInspector())
                    .stripeSize(getStripeSize())
                    .bufferSize(getBufferSize()));
  }

  @Override
  public void exportFiles(HDSFileAccess store, long bucketKey, Set<String> filesAdded, Set<String> filesRemoved) throws IOException {

    HDSFileReader reader = null;
    HDHTCodec<EVENT> codec = getCodec();

    // Holds a map of relative ORC file paths to ORC Writer for that file
    Map<Path, Writer> orcWriters = Maps.newHashMap();


    // Write key/value entries from HDHT files to ORC files
    for (String fileName: filesAdded) {
      reader = store.getReader(bucketKey, fileName);
      TreeMap<Slice, byte[]> fileData = new TreeMap<Slice, byte[]>();
      reader.readFully(fileData);
      reader.close();
      for (Map.Entry<Slice, byte[]> entry: fileData.entrySet()) {
        EVENT record = codec.fromKeyValue(entry.getKey(), entry.getValue());
        // Determine Hive partition directory based on the record and append original HDS file name
        Path orcPath = new Path(getHivePartitionPath(record), fileName);
        // Append the contents to specified partition
        Writer writer = orcWriters.get(orcPath);
        if (writer == null) {
          writer = getORCWriter(new Path(getTmpBasePath(), orcPath));
          orcWriters.put(orcPath, writer);
        }
        writer.addRow(record);
      }
    }

    // Close ORC writers
    for (Writer writer: orcWriters.values()) {
      writer.close();
    }

    // Create list of files to delete
    Set<Path> deleteORCFiles = Sets.newHashSet();

    for (String fileName: filesRemoved) {
      reader = store.getReader(bucketKey, fileName);
      TreeMap<Slice, byte[]> fileData = new TreeMap<Slice, byte[]>();
      reader.readFully(fileData);
      reader.close();
      for (Map.Entry<Slice, byte[]> entry : fileData.entrySet()) {
        EVENT record = codec.fromKeyValue(entry.getKey(), entry.getValue());
        // Determine Hive partition directory based on the record and append original HDS file name
        Path orcPath = new Path(getHivePartitionPath(record), fileName);
        deleteORCFiles.add(orcPath);
      }
    }


    // Perform partition swaps

    // TODO - replace partitions by making

    // Perform file removal from DFS
    for (Path orcFilePath: deleteORCFiles ) {
      Path fullPath = new Path(getBasePath(), orcFilePath);
      getFileSystem().delete(fullPath, true);
    }



  }

}
