package com.datatorrent.contrib.kafka;

import java.io.Serializable;

/**
 * @since 2.1.0
 */
public class KafkaPartition implements Serializable
{
  protected static final String DEFAULT_CLUSTERID = "com.datatorrent.contrib.kafka.defaultcluster";
  
  @SuppressWarnings("unused")
  private KafkaPartition()
  {
  }

  public KafkaPartition(String topic, int partitionId)
  {
    this(DEFAULT_CLUSTERID, topic, partitionId);
  }

  public KafkaPartition(String clusterId, String topic, int partitionId)
  {
    super();
    this.clusterId = clusterId;
    this.partitionId = partitionId;
    this.topic = topic;
  }

  /**
   * 
   */
  private static final long serialVersionUID = 7556802229202221546L;
  

  private String clusterId;
  
  private int partitionId;
  
  private String topic;

  public String getClusterId()
  {
    return clusterId;
  }

  public void setClusterId(String clusterId)
  {
    this.clusterId = clusterId;
  }

  public int getPartitionId()
  {
    return partitionId;
  }

  public void setPartitionId(int partitionId)
  {
    this.partitionId = partitionId;
  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((clusterId == null) ? 0 : clusterId.hashCode());
    result = prime * result + partitionId;
    result = prime * result + ((topic == null) ? 0 : topic.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    KafkaPartition other = (KafkaPartition) obj;
    if (clusterId == null) {
      if (other.clusterId != null)
        return false;
    } else if (!clusterId.equals(other.clusterId))
      return false;
    if (partitionId != other.partitionId)
      return false;
    if (topic == null) {
      if (other.topic != null)
        return false;
    } else if (!topic.equals(other.topic))
      return false;
    return true;
  }

  @Override
  public String toString()
  {
    return "KafkaPartition [clusterId=" + clusterId + ", partitionId=" + partitionId + ", topic=" + topic + "]";
  }
  
  
  
}
