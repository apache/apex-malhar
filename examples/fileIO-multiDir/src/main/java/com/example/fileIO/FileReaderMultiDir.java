/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
//package com.datatorrent.lib.io.fs;
package com.example.fileIO;

import java.io.ByteArrayOutputStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.DefaultPartition;

import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

public abstract class FileReaderMultiDir extends AbstractFileInputOperator<String>
{
  // parallel arrays: directories[i] is monitored by partitionCounts[i] partitions
  // which should be contiguous in the list of partitions.
  //
  @NotNull
  private String[] directories;
  @NotNull
  private int[] partitionCounts;

  /**
   * List of monitored directories
   */
  public String[] getDirectories()
  {
    return directories;
  }

  public void setDirectories(String[] v)
  {
    directories = v;
  }

  /**
   * Partition count for each monitored directory
   */
  public int[] getPartitionCounts()
  {
    return partitionCounts;
  }

  public void setPartitionCounts(int[] v)
  {
    partitionCounts = v;
  }

  /**
   * Clone object by serializing and deserializing using Kryo.
   * Note this is different from using {@link Kryo#copy(Object)}, which will attempt to also clone transient fields.
   * (from KryoCloneUtils.java)
   *
   * @param kryo kryo object used to clone objects
   * @param src src object that copy from
   * @return
   */
  @SuppressWarnings("unchecked")
  public static <SRC> SRC cloneObject(Kryo kryo, SRC src)
  {
    kryo.setClassLoader(src.getClass().getClassLoader());
    ByteArrayOutputStream bos = null;
    Output output;
    Input input = null;
    try {
      bos = new ByteArrayOutputStream();
      output = new Output(bos);
      kryo.writeObject(output, src);
      output.close();
      input = new Input(bos.toByteArray());
      return (SRC)kryo.readObject(input, src.getClass());
    } finally {
      IOUtils.closeQuietly(input);
      IOUtils.closeQuietly(bos);
    }
  }

  @Override
  public Collection<Partition<AbstractFileInputOperator<String>>> definePartitions(
      Collection<Partition<AbstractFileInputOperator<String>>> partitions, PartitioningContext context)
  {

    final int prevCount = partitions.size();
    if (1 < prevCount) {    // dynamic repartition not supported in this example
      throw new RuntimeException("Error: Dynamic repartition not supported");
    }

    // compute first and last indices of partitions for each directory
    final int numDirs = directories.length, numParCounts = partitionCounts.length;
    final int[] sliceFirstIndex = new int[numDirs];

    LOG.info("definePartitions: prevCount = {}, directories.length = {}, " +
             "partitionCounts.length = {}", prevCount, numDirs, numParCounts);

    int nPartitions = 0;        // desired number of partitions

    for (int i = 0; i < numDirs; ++i) {
      sliceFirstIndex[i] = nPartitions;
      final int nP = partitionCounts[i];
      LOG.info("definePartitions: i = {}, nP = {}, dir = {}", i, nP, directories[i]);
      nPartitions += nP;
    }


    if (1 == nPartitions) {
      LOG.info("definePartitions: Nothing to do in definePartitions");
      return partitions;    // nothing to do
    }

    if (nPartitions <= 0) {    // error
      final String msg = String.format("Error: Bad number of partitions %d%n", nPartitions);
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
    this.partitionCount = nPartitions;

    LOG.debug("definePartitions: Creating {} partitions", nPartitions);

    AbstractFileInputOperator<String> tempOperator = partitions.iterator().next().getPartitionedInstance();

    /*
     * Create partitions of scanners, scanner's partition method will do state
     * transfer for DirectoryScanner objects.
     */
    Kryo kryo = new Kryo();

    SlicedDirectoryScanner sds = (SlicedDirectoryScanner)scanner;
    List<SlicedDirectoryScanner> scanners = sds.partition(nPartitions, directories,
                                                          partitionCounts);

    // return value: new list of partitions (includes old list)
    List<Partition<AbstractFileInputOperator<String>>> newPartitions
      // = Lists.newArrayListWithExpectedSize(totalCount);
      = new ArrayList(nPartitions);

    // parallel list of storage managers
    Collection<WindowDataManager> newManagers
      //  = Lists.newArrayListWithExpectedSize(totalCount);
      = new ArrayList(nPartitions);

    // setup new partitions
    LOG.info("definePartitions: setting up {} new partitoins with {} monitored directories",
             nPartitions, numDirs);

    final WindowDataManager ism = getWindowDataManager();

    // j is the index of current slice
    int idx = 0;        // index of partition
    for (int j = 0; j < numDirs; ++j) {
      int first = sliceFirstIndex[j];
      int last = first + partitionCounts[j];
      String dir = directories[j];
      LOG.info("definePartitions: first = {}, last = {}, dir = {}", first, last, dir);
      for (int i = first; i < last; ++i) {
        AbstractFileInputOperator<String> oper = cloneObject(kryo, this);
        oper.setDirectory(dir);
        //oper.setpIndex(i);
        SlicedDirectoryScanner scn = (SlicedDirectoryScanner) scanners.get(i);
        scn.setStartIndex(first);
        scn.setEndIndex(last);
        scn.setDirectory(dir);

        oper.setScanner(scn);
        newPartitions.add(new DefaultPartition<>(oper));
        newManagers.add(oper.getWindowDataManager());
      }
    }

    ism.partitioned(newManagers, null);
    LOG.info("definePartition: returning {} partitions", newPartitions.size());
    return newPartitions;
  }

  /**
   * A directory scanner where each instance monitors a different directory
   */
  public static class SlicedDirectoryScanner extends AbstractFileInputOperator.DirectoryScanner
  {
    // Each slice of partitions monitors a single directory.
    // startIndex -- of partition slice
    // endIndex -- of partition slice (1 beyond last index of slice)
    // pIndex --  index of this partition (we don't use the partitionIndex field in
    //   DirectoryScanner since it is private and has no accessors defined; we also use
    //   it differently here.
    //
    int startIndex, endIndex, pIndex;

    // the monitored directory
    String directory;

    // helper method to create the collection of new partitions
    // total -- the total number of desired partitions; must equal sum of pCounts
    // dirs -- list of monitored directories, one per slice
    // pCounts -- number of partitions in each slice
    //
    private List<SlicedDirectoryScanner> partition(int total,
                                                   String[] dirs,
                                                   int[] pCounts) {
      ArrayList<SlicedDirectoryScanner> partitions = new ArrayList(total);
      for (int i = 0; i < total; i++) {
        final SlicedDirectoryScanner that = new SlicedDirectoryScanner();
        that.setFilePatternRegexp(getFilePatternRegexp());
        that.setpIndex(i);
        // startIndex, endIndex and directory set later in definePartitions
        partitions.add(that);
      }
      return partitions;
    }

    @Override
    protected boolean acceptFile(String filePathStr)
    {
      LOG.debug("startIndex = {}, endIndex = {}", startIndex, endIndex);

      int sliceSize = endIndex - startIndex;

      // check if file should be processed by this partition
      if (sliceSize > 1) {
        // find the partition to receive this file
        final int i = filePathStr.hashCode();
        int mod = i % sliceSize;
        if (mod < 0) {
          mod += sliceSize;
        }
        LOG.debug("partitionIndex = {}, startIndex = {}, endIndex = {}, sliceSize = {}, " +
                  "filePathStr = {}, hashcode = {}, mod = {}",
                  pIndex, startIndex, endIndex, sliceSize, filePathStr, i, mod);

        if ((startIndex + mod) != pIndex) {
          return false;
        }
      }

      // check if file matches regex filter
      Pattern regex = this.getRegex();
      if (regex != null) {
        Matcher matcher = regex.matcher(filePathStr);
        if (!matcher.matches()) {
          return false;
        }
      }
      return true;
    }  // acceptFile

    // getters and setters
    public String getDirectory() { return directory; }
    public void setDirectory(String v) { directory = v; }
    public int getStartIndex() { return startIndex; }
    public void setStartIndex(int v) { startIndex = v; }
    public int getEndIndex() { return endIndex; }
    public void setEndIndex(int v) { endIndex = v; }
    public int getpIndex() { return pIndex; }
    public void setpIndex(int v) { pIndex = v; }

  }    // SlicedDirectoryScanner

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileInputOperator.class);
}
