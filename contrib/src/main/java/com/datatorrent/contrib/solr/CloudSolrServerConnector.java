package com.datatorrent.contrib.solr;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.apache.solr.client.solrj.impl.CloudSolrServer;

/**
 * Initializes CloudServer instance of Solr Server.<br>
 * <br>
 * Properties:<br>
 * zookeeperHost - The client endpoint of the zookeeper quorum containing the cloud state, in the form HOST:PORT<br>
 * updateToLeader - sends updates only to leaders - defaults to true
 *
 * @since 2.0.0
 */
public class CloudSolrServerConnector extends SolrServerConnector
{

  @NotNull
  private String zookeeperHost;
  private boolean updateToLeader;

  @Override
  public void connect() throws IOException
  {
    solrServer = new CloudSolrServer(zookeeperHost, updateToLeader);
  }

  public void setSolrZookeeperHost(String solrServerURL)
  {
    this.zookeeperHost = solrServerURL;
  }

  /*
   * The client endpoint of the zookeeper quorum containing the cloud state, in the form HOST:PORT
   * Gets the zookeeper host
   */
  public String getSolrZookeeperHost()
  {
    return zookeeperHost;
  }

  public void setUpdateToLeader(boolean updateToLeader)
  {
    this.updateToLeader = updateToLeader;
  }

  /*
   * Sends updates only to leaders - defaults to true
   * Gets boolean value of updateToLeader
   */
  public boolean getUpdateToLeader()
  {
    return updateToLeader;
  }

}
