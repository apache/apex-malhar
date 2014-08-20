package com.datatorrent.lib.io.fs;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFSDirectoryInputOperator.DirectoryScanner;
import com.google.common.collect.Sets;
import java.io.*;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file input operator.
 * 
 * Allows the user to read uncompressed and gzip compressed input files. When
 * gzip compressed input files are read they are first copied to the specified
 * backup directory.
 * 
 * @since 1.0.4
 */
public abstract class FSInputOperator<T> extends AbstractThroughputHashFSDirectoryInputOperator<T>
{
  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();

  private int replayCountdown;
  private String backupDirectory;

  private transient BufferedReader br;
  private transient FileSystem dstFs;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    if (backupDirectory != null) {
      try {
        dstFs = FileSystem.newInstance(new Path(backupDirectory).toUri(), configuration);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    if (dstFs != null) {
      try {
        dstFs.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    if (path.toString().toLowerCase().endsWith(".gz")) {
      try {
        is = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, is);
        if (dstFs != null) {
          FileUtil.copy(fs, path, dstFs, new Path(backupDirectory, path.getName()), false, configuration);
        }
      }
      catch (CompressorException ce) {
        throw new IOException(ce);
      }
    }
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    br.close();
    super.closeFile(is);
  }

  public T readEntity() throws IOException
  {
    return readEntityFromReader(br);
  }
  
  protected abstract T readEntityFromReader(BufferedReader reader) throws IOException;

  @Override
  protected void emit(T tuple)
  {
    output.emit(tuple);
  }

  public int getReplayCountdown()
  {
    return replayCountdown;
  }

  public void setReplayCountdown(int count)
  {
    this.replayCountdown = count;
  }

  public void setBackupDirectory(String backupDirectory)
  {
    this.backupDirectory = backupDirectory;
  }

  public String getBackupDirectory()
  {
    return this.backupDirectory;
  }

  public static class HashCodeBasedDirectoryScanner extends AbstractFSDirectoryInputOperator.DirectoryScanner
  {
    private static final long serialVersionUID = 6010948077851507418L;
    private static final Logger LOG = LoggerFactory.getLogger(HashCodeBasedDirectoryScanner.class);

    private String filePatternRegexp;
    private transient Pattern regex = null;
    private int partitionIndex;
    private int partitionCount;

    @Override
    public String getFilePatternRegexp()
    {
      return filePatternRegexp;
    }

    @Override
    public void setFilePatternRegexp(String filePatternRegexp)
    {
      this.filePatternRegexp = filePatternRegexp;
      this.regex = null;
    }

    @Override
    public LinkedHashSet<Path> scan(FileSystem fs, Path filePath, Set<String> consumedFiles)
    {
      if (filePatternRegexp != null && this.regex == null) {
        this.regex = Pattern.compile(this.filePatternRegexp);
      }

      LinkedHashSet<Path> pathSet = Sets.newLinkedHashSet();
      try {
        LOG.debug("Scanning {} with pattern {}", filePath, this.filePatternRegexp);
        FileStatus[] files = fs.listStatus(filePath);
        for (FileStatus status : files) {
          Path path = status.getPath();
          String filePathStr = path.toString();

          if (consumedFiles.contains(filePathStr)) {
            continue;
          }

          if (acceptFile(filePathStr)) {
            LOG.debug("Found {}", filePathStr);
            pathSet.add(path);
          }
          else {
            // don't look at it again
            consumedFiles.add(filePathStr);
          }
        }
      }
      catch (FileNotFoundException e) {
        LOG.warn("Failed to list directory {}", filePath, e);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      return pathSet;
    }

    @Override
    protected boolean acceptFile(String filePathStr)
    {
      if (regex != null) {
        Matcher matcher = regex.matcher(filePathStr);
        if (!matcher.matches()) {
          return false;
        }
        if (partitionCount > 1) {
          int i = filePathStr.hashCode();
          int mod = i % partitionCount;
          if (mod < 0) {
            mod += partitionCount;
          }
          LOG.debug("Validation {} {} {}", filePathStr, i, mod);

          if (mod != partitionIndex) {
            return false;
          }
        }
      }
      return true;
    }

    @Override
    protected DirectoryScanner createPartition(int partitionIndex, int partitionCount)
    {
      HashCodeBasedDirectoryScanner that = new HashCodeBasedDirectoryScanner();
      that.filePatternRegexp = this.filePatternRegexp;
      that.partitionIndex = partitionIndex;
      that.partitionCount = partitionCount;
      return that;
    }

    @Override
    public String toString()
    {
      return "HashCodeBasedDirectoryScanner [filePatternRegexp=" + filePatternRegexp + "]";
    }

  }

}
