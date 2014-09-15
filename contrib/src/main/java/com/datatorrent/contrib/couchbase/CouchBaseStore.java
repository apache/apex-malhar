package com.datatorrent.contrib.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.datatorrent.lib.db.Connectable;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author prerna
 */
public class CouchBaseStore implements Connectable {

    protected static final Logger logger = LoggerFactory.getLogger(CouchBaseStore.class);
    protected transient String bucket;
    protected transient String password;
    protected transient CouchbaseClient client;
    protected String uriString;
    List<String> uriList = new ArrayList<String>();
    List<URI> baseURIs = new ArrayList<URI>();

    public CouchBaseStore() {
        client = null;
        bucket = "default";
        password = "";

    }

    public CouchbaseClient getInstance() {
        return client;
    }

    public void addNodes(URI url) {
        baseURIs.add(url);
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

    public void setUriString(String uriString) {
        logger.info("In setter method of URI");
        this.uriString = uriString;
        String[] tokens = uriString.split(",");
        uriList.addAll(Arrays.asList(tokens)); 
    }

    @Override
    public void connect() throws IOException {
        URI uri = null;
        for (String url : uriList) {
            try {
                uri = new URI("http",url, "/pools", null, null);
            } catch (URISyntaxException ex) {
               logger.error(ex.getMessage());
            }
           baseURIs.add(uri);
        }
        try {

            client = new CouchbaseClient(new CouchbaseConnectionFactoryBuilder()
                    .setViewTimeout(30) // set the timeout to 30 seconds
                    .setViewWorkerSize(5) // use 5 worker threads instead of one
                    .setViewConnsPerNode(20) // allow 20 parallel http connections per node in the cluster
                    .buildCouchbaseConnection(baseURIs, bucket, password));
          //client = new CouchbaseClient(baseURIs, "default", "");
        } catch (IOException e) {
            logger.error("Error connecting to Couchbase: " + e.getMessage());

        }

    }

    @Override
    public boolean connected() {
        // Not applicable for Couchbase
        return false;
    }

    @Override
    public void disconnect() throws IOException {
        client.shutdown(60, TimeUnit.SECONDS);
    }

}
