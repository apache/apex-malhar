package com.datatorrent.lib.io.fs;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import java.io.*;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

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
}
