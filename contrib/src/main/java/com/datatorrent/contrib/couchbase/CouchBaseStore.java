 
package com.datatorrent.contrib.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.datatorrent.lib.db.Connectable;
import com.couchbase.client.CouchbaseConnectionFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author prerna
 */


public class CouchBaseStore implements  Connectable{
    
  protected static final Logger logger = LoggerFactory.getLogger(CouchBaseStore.class);
  private String bucket;
  private String password;
  protected transient CouchbaseClient client;
  List<URI> baseURIs = new ArrayList<URI>(); 
 
  public CouchBaseStore(){
      client = null;
      baseURIs = new ArrayList<URI>();
      bucket = "default";
      password="";
      
  }
 
  
  public CouchbaseClient getInstance() {
    return client;
  }
 
  public void addNodes(String url)
  {
    // Add one or more nodes of your cluster (exchange the IP with yours)
   // nodes.add(URI.create("http://127.0.0.1:8091/pools"));
      URI base = null;
      try {
          base = new URI(String.format("http://%s:8091/pools",url));
      } catch (URISyntaxException ex) {
          java.util.logging.Logger.getLogger(CouchBaseStore.class.getName()).log(Level.SEVERE, null, ex);
      }
        
        baseURIs.add(base);
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
         java.util.logging.Logger.getLogger(CouchBaseStore.class.getName()).log(Level.SEVERE,"In connect method of store");
         CouchbaseConnectionFactory cf = new CouchbaseConnectionFactory(baseURIs, "default", "");
         client = new CouchbaseClient((CouchbaseConnectionFactory) cf);
    } catch (IOException e) {
      System.err.println("Error connecting to Couchbase: " + e.getMessage());
      
    }
 /*   try {
          // Store a Document
          client.set("my-first-document", "Hello Couchbase!").get();
      } catch (InterruptedException ex) {
          java.util.logging.Logger.getLogger(CouchBaseStore.class.getName()).log(Level.SEVERE, null, ex);
      } catch (ExecutionException ex) {
          java.util.logging.Logger.getLogger(CouchBaseStore.class.getName()).log(Level.SEVERE, null, ex);
      }
 
    // Retreive the Document and print it
    System.out.println(client.get("my-first-document"));
 
    // Shutting down properly
    client.shutdown();*/
    
  }
  
  
 /* public static void main(String[] args) {
      try {
          // (Subset) of nodes in the cluster to establish a connection
          List<URI> hosts = null;
          try {
              hosts = Arrays.asList(
                      new URI("http://127.0.0.1:8091/pools")
              );} catch (URISyntaxException ex) {
                  java.util.logging.Logger.getLogger(CouchBaseStore.class.getName()).log(Level.SEVERE, null, ex);
              }
          // Name of the Bucket to connect to
          String bucket = "default";
          
          // Password of the bucket (empty) string if none
          String password = "";
          
          // Connect to the Cluster
          CouchbaseClient client=null;
          try {
              client = new CouchbaseClient(hosts, bucket, password);
          } catch (IOException ex) {
              java.util.logging.Logger.getLogger(CouchBaseStore.class.getName()).log(Level.SEVERE, null, ex);
          }
          
          Gson gson = new Gson();
          
          User user1 = new User("John", "Doe");
          
          
          client.set("user1", user1).get();
          
         
          
          // Retreive the Document and print it
          System.out.println(client.get("user1"));
          System.out.println(client.get("user1").getClass());
          System.out.println(client.get("test"));
          System.out.println(client.get("test").getClass());
          
          // Shutting down properly
          client.shutdown();
          
      } catch (InterruptedException ex) {
          java.util.logging.Logger.getLogger(CouchBaseStore.class.getName()).log(Level.SEVERE, null, ex);
      } catch (ExecutionException ex) {
          java.util.logging.Logger.getLogger(CouchBaseStore.class.getName()).log(Level.SEVERE, null, ex);
      }
      
  }*/
      
  @Override
  public boolean connected() {
    // Not applicable for Couchbase
    return false;
  }

  @Override
  public void disconnect() throws IOException {
    java.util.logging.Logger.getLogger(CouchBaseStore.class.getName()).log(Level.SEVERE,"Diconnect called");
    client.shutdown(60, TimeUnit.SECONDS);
    java.util.logging.Logger.getLogger(CouchBaseStore.class.getName()).log(Level.SEVERE,"client shutdown succeeded");
  }

   
}
