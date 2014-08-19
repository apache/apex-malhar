package com.datatorrent.lib.io.fs;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.StatsListener;
import com.datatorrent.common.util.Pair;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author tfarkas
 */


public abstract class AbstractThroughputHashFSDirectoryInputOperator<T> extends AbstractFSDirectoryInputOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractThroughputHashFSDirectoryInputOperator.class);
  
  private long repartitionInterval = 60L * 1000L;
  private int preferredMaxPendingFilesPerOperator = 10;
  
  private transient long lastRepartition = 0;
  
  public void setRepartitionInterval(long repartitionInterval)
  {
    this.repartitionInterval = repartitionInterval;
  }

  public long getRepartitionInterval()
  {
    return repartitionInterval;
  }

  public void setPreferredMaxPendingFilesPerOperator(int pendingFilesPerOperator)
  {
    this.preferredMaxPendingFilesPerOperator = pendingFilesPerOperator;
  }

  public int getPreferredMaxPendingFilesPerOperator()
  {
    return preferredMaxPendingFilesPerOperator;
  }
  
  @Override
  public Collection<Partition<AbstractFSDirectoryInputOperator<T>>> definePartitions(Collection<Partition<AbstractFSDirectoryInputOperator<T>>> partitions, int incrementalCapacity)
  {
    lastRepartition = System.currentTimeMillis();
    LOG.info("PARTITION SIZE: " + partitions.size());
    
    //
    // Build collective state from all instances of the operator.
    //
    
    Set<String> totalProcessedFiles = new HashSet<String>();
    List<FailedFile> currentFiles = new LinkedList<FailedFile>();
    List<DirectoryScanner> oldscanners = new LinkedList<DirectoryScanner>();
    List<FailedFile> totalFailedFiles = new LinkedList<FailedFile>();
    List<Path> totalPendingFiles = new LinkedList<Path>();
    for(Partition<AbstractFSDirectoryInputOperator<T>> partition : partitions) {
      AbstractFSDirectoryInputOperator<T> oper = partition.getPartitionedInstance();
      totalProcessedFiles.addAll(oper.processedFiles);
      totalFailedFiles.addAll(oper.failedFiles);
      totalPendingFiles.addAll(oper.pendingFiles);
      oper.pendingFiles.size();
      if (oper.currentFile != null)
        currentFiles.add(new FailedFile(oper.currentFile, oper.offset));
      oldscanners.add(oper.getScanner());
    }
    
    int totalFileCount = currentFiles.size() + totalFailedFiles.size() + totalPendingFiles.size();
    
    int newOperatorCount = totalFileCount / preferredMaxPendingFilesPerOperator;
    
    if(totalFileCount % preferredMaxPendingFilesPerOperator > 0)
    {
      newOperatorCount++;
    }
    
    if(newOperatorCount > partitionCount)
    {
      newOperatorCount = partitionCount;
    }
    
    //if(newOperatorCount == partitions.size())
    //{
    //  return partitions;
    //}
    
    Kryo kryo = new Kryo();
    
    if(newOperatorCount == 0)
    {
      Collection<Partition<AbstractFSDirectoryInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(1);

      AbstractThroughputHashFSDirectoryInputOperator<T> operator;
      
      operator = kryo.copy(this);
  
      
      operator.processedFiles.addAll(totalProcessedFiles);
      operator.currentFile = null;
      operator.offset = 0;
      operator.pendingFiles.clear();
      operator.pendingFiles.addAll(totalPendingFiles);
      operator.failedFiles.clear();
      operator.failedFiles.addAll(totalFailedFiles);
      operator.unfinishedFiles.clear();
      operator.unfinishedFiles.addAll(currentFiles);
      
      List<DirectoryScanner> scanners = scanner.partition(1, oldscanners);
      
      operator.setScanner(scanners.get(0));
      
      newPartitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<T>>(operator));
      
      return newPartitions;
    }
    
    
    //
    // Create partitions of scanners, scanner's partition method will do state
    // transfer for DirectoryScanner objects.
    //
    List<DirectoryScanner> scanners = scanner.partition(newOperatorCount, oldscanners);

    Collection<Partition<AbstractFSDirectoryInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(newOperatorCount);
    for (int i=0; i<scanners.size(); i++) {
      AbstractThroughputHashFSDirectoryInputOperator<T> oper = kryo.copy(this);
      DirectoryScanner scn = scanners.get(i);
      oper.setScanner(scn);

      // Do state transfer for processed files.
      oper.processedFiles.addAll(totalProcessedFiles);

      // set current scanning directory and offset
      oper.unfinishedFiles.clear();
      oper.currentFile = null;
      oper.offset = 0;
      Iterator<FailedFile> unfinishedIter = currentFiles.iterator();
      while(unfinishedIter.hasNext()) {
        FailedFile unfinishedFile = unfinishedIter.next();
        if (scn.acceptFile(unfinishedFile.path)) {
          oper.unfinishedFiles.add(unfinishedFile);
          unfinishedIter.remove();
        }
      }

      // transfer failed files 
      oper.failedFiles.clear();
      Iterator<FailedFile> iter = totalFailedFiles.iterator();
      while (iter.hasNext()) {
        FailedFile ff = iter.next();
        if (scn.acceptFile(ff.path)) {
          oper.failedFiles.add(ff);
          iter.remove();
        }
      }
      
      oper.pendingFiles.clear();
      Iterator<Path> pendingFilesIterator = totalPendingFiles.iterator();
      while(pendingFilesIterator.hasNext())
      {
        Path path = pendingFilesIterator.next();
        if(scn.acceptFile(path.toString()))
        {
          oper.pendingFiles.add(path);
          pendingFilesIterator.remove();
        }
      }

      newPartitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<T>>(oper));
    }

    LOG.info("definePartitions called returning {} partitions", newPartitions.size());
    
    return newPartitions;
  }
  
  @Override
  public void partitioned(Map<Integer, Partition<AbstractFSDirectoryInputOperator<T>>> partitions)
  {
  }
  
  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    Response response = new Response();
    response.repartitionRequired = false;
    
    if(System.currentTimeMillis() - repartitionInterval > lastRepartition)
    {
      response.repartitionRequired = true;
      return response;
    }

    return response;
  }
}
