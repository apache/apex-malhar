/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class TupleRecorder
{
  public transient FSDataOutputStream fsOutput;
  public FSDataOutputStream indexOs;
  public int bytesPerFile;
  public String basePath =  "";
  public transient String hdfsFile;
  public String indexFile = "index.txt";
  public String metaFile = "meta.txt";
  public int fileParts=0;
  public int tupleCount = 0;
  public HashMap<String, PortInfo> portMap = new HashMap<String, PortInfo>(); // used for output portInfo <name, id> map
  public transient long windowId;
  public RecordInfo recordInfo;
  FileSystem fs;

  /* defined for json information */
  public static class PortInfo {
    public String name;
    public String type;
    public int id;
  }
    /* defined for json information */
  public static class RecordInfo {
    public long startTime;
    public String recordingName;
  }

  public void setBytesPerFile(int bytes) {
    bytesPerFile = bytes;
  }
  public void setBasePath(String path) {
    basePath = path;
  }
  public String getBasePath() {
    return basePath;
  }
  public void setIndexFile(String name) {
    indexFile = name;
  }
  public void setMetaFile(String name) {
    metaFile = name;
  }
  public void addPortInfoMap(String port, PortInfo portInfo) {
    portMap.put(port, portInfo);
  }
  public HashMap<String, PortInfo> getPortInfoMap() {
    return portMap;
  }

  public void teardown() {
    if( indexOs != null ) {
      try {
        indexOs.close();
      }
      catch (IOException ex) {
        System.out.println(ex.toString());
      }
    }
    if( fsOutput != null ) {
      try {
        fsOutput.close();
      }
      catch (IOException ex) {
        System.out.println(ex.toString());
      }
    }
  }

  public void setup() {
    try {
      Path pa = new Path(basePath+metaFile);
      fs = FileSystem.get(pa.toUri(), new Configuration());
      FSDataOutputStream metaOs = fs.create(pa);

      ObjectMapper mapper = new ObjectMapper();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();

      for( PortInfo pi : portMap.values()) {
      mapper.writeValue(bos, pi);
      bos.write("\n".getBytes());
      }

      recordInfo = new RecordInfo();
      recordInfo.startTime = System.currentTimeMillis();
      recordInfo.recordingName = "somename";
      mapper.writeValue(bos, recordInfo);
      bos.write("\n".getBytes());

      metaOs.write(bos.toByteArray());
      metaOs.hflush();
      metaOs.close();

      pa = new Path(basePath+indexFile);
//      indexOs = fs.create(pa, true, 10);
      indexOs = fs.create(pa);

    }
    catch (IOException ex) {
      System.out.println(ex.getMessage());
      System.out.println("111111111111111111111111111");
      System.out.println(ex.toString());
      ex.printStackTrace();
    }
  }

  public void writeBeginWindow(long windowId) {
    this.windowId = windowId;
    try {
      if( fsOutput == null || fsOutput.getPos() > bytesPerFile ) {
        hdfsFile = basePath+"part"+fileParts+".txt";
        Path path = new Path(hdfsFile);
        fsOutput = fs.create(path);
        fileParts++;
        indexOs.write(("B:"+windowId+":T:"+tupleCount+":"+hdfsFile+"\n").getBytes());
        indexOs.hflush();
      }
      fsOutput.write(("B:"+windowId+"\n").getBytes());
      fsOutput.hflush();
    }
    catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  public void writeEndWindow() {
    try {
      fsOutput.write(("E:"+windowId+"\n").getBytes());
      fsOutput.hflush();
      if( fsOutput.getPos() > bytesPerFile ) {
        fsOutput.close();
      }
    }
    catch (JsonGenerationException ex) {
      System.out.println(ex.toString());
    }
    catch (JsonMappingException ex) {
      System.out.println(ex.toString());
    }
    catch (IOException ex) {
      System.out.println(ex.toString());
    }
  }

  public void writeTuple(Object obj, String port){
    try {
      ObjectMapper mapper = new ObjectMapper();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      mapper.writeValue(bos, obj);
      bos.write("\n".getBytes());

      PortInfo pi = portMap.get(port);
      String str = "T:"+tupleCount+":"+pi.id+":"+bos.size()+":";
      fsOutput.write(str.getBytes());
      fsOutput.write(bos.toByteArray());
      fsOutput.hflush();
      ++ tupleCount;
    }
    catch (JsonGenerationException ex) {
      System.out.println(ex.toString());
    }
    catch (JsonMappingException ex) {
      System.out.println(ex.toString());
    }
    catch (IOException ex) {
      System.out.println(ex.toString());
    }
  }

}
