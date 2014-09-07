package com.datatorrent.contrib.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.TransactionableStore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author prerna
 */


public class CouchBaseWindowStore extends CouchBaseStore implements TransactionableStore{
    
  private static final transient Logger logger = LoggerFactory.getLogger(CouchBaseStore.class);
  private transient String key;
  private transient Object value;
  
  private transient byte[] keyBytes;
  private transient byte[] valueBytes;
  private String bucket;
  private String password;
 
  private transient String lastWindowValue;
  private transient byte[] lastWindowValueBytes;
  private ObjectMapper mapper ;
  
  public CouchBaseWindowStore() {
    mapper = new ObjectMapper();
    bucket = "metadata";
    password="";
      
    constructKeys();
  }

   public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
    constructKeys();
  }

  public Object getValue() {
    return getInstance().get(key);
  }

  public void setValue(Object value) {
    this.value = value;
    constructKeys();
  }

  public void setBucket(String bucketName) {
    this.bucket = bucketName;
  }

  /**
   * setter for password
   * 
   * @param password
   */
  public void setPassword(String password) {
    this.password = password;
  }
  
  //public void connect() throws IOException
  @Override
  public void connect() throws IOException {
    try {
        client = new CouchbaseClient(nodes,bucket,password);
    } catch (IOException e) {
      System.err.println("Error connecting to Couchbase: " + e.getMessage());
      System.exit(1);
    }
  }
  
    @Override
    public long getCommittedWindowId(String appId, int operatorId) {
        
         byte[] value = null;
    
    String key = appId + "_" + operatorId + "_" + lastWindowValue;
    //lastWindowValueBytes = key.getBytes();
    value = (byte[])getInstance().get(key);
    if (value != null) {
      long longval = toLong(value);
      return longval;
    }
    return -1;
    }

    @Override
  public void storeCommittedWindowId(String appId, int operatorId,long windowId) {
    byte[] WindowIdBytes = Bytes.toBytes(windowId);
    String key = appId + "_" + operatorId + "_" + lastWindowValue;
    //lastWindowValueBytes = key.getBytes();
  
      try {
          client.set(key,WindowIdBytes ).get();
      } catch (InterruptedException ex) {
          java.util.logging.Logger.getLogger(CouchBaseWindowStore.class.getName()).log(Level.SEVERE, null, ex);
      } catch (ExecutionException ex) {
          java.util.logging.Logger.getLogger(CouchBaseWindowStore.class.getName()).log(Level.SEVERE, null, ex);
      }
  
  }
    @Override
    public void removeCommittedWindowId(String appId, int operatorId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void beginTransaction() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void commitTransaction() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void rollbackTransaction() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isInTransaction() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void constructKeys() {
        if(key!=null)
        {
            keyBytes = key.getBytes();
            valueBytes = value.toString().getBytes();  //use json converter
        }
    }
    
     public static byte[] toBytes(String object) 
   {
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    DataOutputStream dos=new DataOutputStream(baos);
    byte[] result=null;
    try {
      dos.writeChars(object);
      result=baos.toByteArray();
      dos.close();   
    } catch (IOException e) {
      logger.error("error converting to byte array");
      DTThrowable.rethrow(e);
    }
    return result;
  }


  public static String toString(byte[] b){
    ByteArrayInputStream baos=new ByteArrayInputStream(b);
    DataInputStream dos=new DataInputStream(baos);
    String result=null;
    try {
      result = dos.readLine();
      dos.close();
    } catch (IOException e) {
      logger.error("error converting to long");
      DTThrowable.rethrow(e);
    }
    return result;
  }
  
   public static long toLong(byte[] b){
    ByteArrayInputStream baos=new ByteArrayInputStream(b);
    DataInputStream dos=new DataInputStream(baos);
    long result=0;
    try {
      result = dos.readLong();
      dos.close();
    } catch (IOException e) {
      logger.error("error converting to long");
      DTThrowable.rethrow(e);
    }
    return result;
  }

}
