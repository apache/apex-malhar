///*
// * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.datatorrent.contrib.hive;
//
//import com.datatorrent.common.util.DTThrowable;
//import com.datatorrent.lib.db.TransactionableStore;
//import java.io.*;
//import javax.validation.constraints.NotNull;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///*
// * Hive Meta Store which provides Transactional properties in Hive Store.
// */
//public class HiveMetaStore extends HiveStore implements TransactionableStore
//{
//  private static final Logger logger = LoggerFactory.getLogger(HiveMetaStore.class);
//
//  //public static String DEFAULT_META_TABLE = "dt_meta";
//  public static String DEFAULT_META_FILE = "dt_file";
//  protected transient FileSystem fsMeta;
//  //protected Configuration conf;
//
//  @NotNull
//  private String fileMeta;
//
//  public HiveMetaStore()
//  {
//    super();
//    // metaTable = DEFAULT_META_TABLE;
//
//    fileMeta = DEFAULT_META_FILE;
//  }
//
//  public void init(){
//    if (fsMeta == null) {
//      try {
//        fsMeta = FileSystem.newInstance(new Path(filepath).toUri(), new Configuration());
//      }
//      catch (IOException ex) {
//        throw new RuntimeException(ex);
//      }
//    }
//
//  }
//
//  public void destroy(){
//    if(fsMeta!=null)
//      try {
//        fsMeta.close();
//    }
//    catch (IOException ex) {
//      logger.info(HiveMetaStore.class.getName()+ ex);
//      DTThrowable.rethrow(ex);
//    }
//  }
//
//  /**
//   * Sets the name of the window column.<br/>
//   * <b>Default:</b> {@value #DEFAULT_WINDOW_COL}
//   *
//   * @param windowColumn window column name.
//   */
//  @Override
//  public void storeCommittedWindowId(String appId, int operatorId, long currentWindowId)
//  {
//    Path metaFilePath = new Path(filepath + "/" + appId + "/" + operatorId + "/" + fileMeta);
//    try {
//      /*if (fsMeta.exists(metaFilePath)) {
//       fsMeta.delete(metaFilePath, true);
//       fsMetaOutput = fsMeta.create(metaFilePath);
//       logger.debug("creating after deleting {} ", metaFilePath);
//       }
//       else {*/
//      FSDataOutputStream fsMetaOutput = fsMeta.create(metaFilePath);
//      logger.debug("creating {}", metaFilePath);
//      //}
//
//     // fsMetaOutput.writeLong(currentWindowId);
//      // fsMetaOutput.writeChars("\n");
//      // fsMetaOutput.writeChars(appId);
//      //  fsMetaOutput.writeChars("\n");
//      fsMetaOutput.writeLong(currentWindowId);
//      if (fsMetaOutput != null) {
//        fsMetaOutput.close();
//        fsMetaOutput = null;
//      }
//    }
//    catch (IOException ex) {
//      logger.debug(HiveMetaStore.class.getName() + ex.getCause());
//      throw new RuntimeException(ex);
//    }
//
//    /*try {
//     stmtMetaInsert = getConnection().createStatement();
//     stmtMetaInsert.execute("load data inpath '" + metaFilePath + "' into table " + metaTable);
//     }
//     catch (SQLException ex) {
//     logger.info(HiveMetaStore.class.getName() + ex.getCause());
//     }*/
//  }
//
//  @Override
//  public long getCommittedWindowId(String appId, int operatorId)
//  {
//    FSDataInputStream inputStream = null;
//    Long lastWindow = null;
//    Path metaFilePath = new Path(filepath + "/" + appId + "/" + operatorId + "/" + fileMeta);
//
//    try {
//      if (fsMeta.exists(metaFilePath)) {
//        inputStream = fsMeta.open(metaFilePath);
//        lastWindow = inputStream.readLong();
//      }
//    }
//    catch (IOException ex) {
//      logger.info(ex.getMessage());
//      DTThrowable.rethrow(ex);
//    }
//    finally{
//      if (inputStream != null) {
//      try {
//        inputStream.close();
//      }
//      catch (IOException ex) {
//        logger.info(HiveMetaStore.class.getName()+ ex);
//      }
//      inputStream = null;
//    }
//    }
//
//    if (lastWindow == null) {
//      return -1L;
//    }
//    else {
//      return lastWindow;
//    }
//  }
//
//  @Override
//  public void removeCommittedWindowId(String appId, int operatorId)
//  {
//  }
//
//  @Override
//  public void beginTransaction()
//  {
//  }
//
//  @Override
//  public void commitTransaction()
//  {
//  }
//
//  @Override
//  public void rollbackTransaction()
//  {
//  }
//
//  @Override
//  public boolean isInTransaction()
//  {
//    return false;
//  }
//
//}
