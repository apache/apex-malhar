package com.datatorrent.contrib.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import java.util.concurrent.TimeUnit;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.TransactionableStore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author prerna
 */
public class CouchBaseWindowStore extends CouchBaseStore implements TransactionableStore
{

  private static final Logger logger = LoggerFactory.getLogger(CouchBaseWindowStore.class);
  private transient String key;
  private transient Object value;
  private static final transient String DEFAULT_LAST_WINDOW_PREFIX = "last_window";
  private transient byte[] keyBytes;
  private transient byte[] valueBytes;

  private transient String lastWindowValue;
  private transient byte[] lastWindowValueBytes;
  protected transient CouchbaseClient clientMeta;

  public CouchBaseWindowStore()
  {
    clientMeta = null;
    lastWindowValue = DEFAULT_LAST_WINDOW_PREFIX;
  }

  public String getKey()
  {
    return key;
  }

  public void setKey(String key)
  {
    this.key = key;
  }

  public Object getValue()
  {
    return clientMeta.get(key);
  }

  public void setValue(Object value)
  {
    this.value = value;
  }

  @Override
  public void setBucket(String bucketName)
  {
    super.setBucket(bucketName);
  }

  @Override
  public CouchbaseClient getInstance()
  {
    return client;
  }

  /**
   * setter for password
   *
   * @param password
   */
  @Override
  public void setPassword(String password)
  {
    super.setPassword(password);
  }

  //public void connect() throws IOException
  @Override
  public void connect() throws IOException
  {
    super.connect();

    try {
      CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
      cfb.setOpTimeout(timeout);  // wait up to 10 seconds for an operation to succeed
      cfb.setOpQueueMaxBlockTime(blockTime); // wait up to 10 second when trying to enqueue an operation
      clientMeta = new CouchbaseClient(cfb.buildCouchbaseConnection(baseURIs, bucket, password));
    }
    catch (IOException e) {
      logger.error("Error connecting to Couchbase: " + e.getMessage());
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    byte[] value = null;
    String key = appId + "_" + operatorId + "_" + lastWindowValue;
    value = (byte[])clientMeta.get(key);
    if (value != null) {
      long longval = toLong(value);
      return longval;
    }
    return -1;
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId, long windowId)
  {
    byte[] WindowIdBytes = toBytes(windowId);
    String key = appId + "_" + operatorId + "_" + lastWindowValue;
    try {
      java.util.logging.Logger.getLogger(CouchBaseWindowStore.class.getName()).log(Level.SEVERE, baseURIs.toString());
      clientMeta.set(key, WindowIdBytes).get();
    }
    catch (InterruptedException ex) {
      java.util.logging.Logger.getLogger(CouchBaseWindowStore.class.getName()).log(Level.SEVERE, null, ex);
    }
    catch (ExecutionException ex) {
      java.util.logging.Logger.getLogger(CouchBaseWindowStore.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
    // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void beginTransaction()
  {
    // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void commitTransaction()
  {
    //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void rollbackTransaction()
  {
    // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isInTransaction()
  {
    //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    return false;
  }

  public static byte[] toBytes(String object)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] result = null;
    try {
      dos.writeChars(object);
      result = baos.toByteArray();
      dos.close();
    }
    catch (IOException e) {
      logger.error("error converting to byte array");
      DTThrowable.rethrow(e);
    }
    return result;
  }

  public static String toString(byte[] b)
  {
    ByteArrayInputStream baos = new ByteArrayInputStream(b);
    DataInputStream dos = new DataInputStream(baos);
    String result = null;
    try {
      result = dos.readLine();
      dos.close();
    }
    catch (IOException e) {
      logger.error("error converting to long");
      DTThrowable.rethrow(e);
    }
    return result;
  }

  public static long toLong(byte[] b)
  {
    ByteArrayInputStream baos = new ByteArrayInputStream(b);
    DataInputStream dos = new DataInputStream(baos);
    long result = 0;
    try {
      result = dos.readLong();
      dos.close();
    }
    catch (IOException e) {
      logger.error("error converting to long");
      DTThrowable.rethrow(e);
    }
    return result;
  }

  public static byte[] toBytes(long l)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.SIZE / 8);
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] result = null;
    try {
      dos.writeLong(l);
      result = baos.toByteArray();
      dos.close();
    }
    catch (IOException e) {
      logger.error("error converting to byte array");
      DTThrowable.rethrow(e);
    }
    return result;
  }

  @Override
  public void disconnect() throws IOException
  {
    clientMeta.shutdown(60, TimeUnit.SECONDS);
    super.disconnect();
  }

}

