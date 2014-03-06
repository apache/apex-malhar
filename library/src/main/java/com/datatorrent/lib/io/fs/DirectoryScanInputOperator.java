/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.DTThrowable;
import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;
import javax.validation.constraints.NotNull;
import org.apache.commons.io.FilenameUtils;
import org.python.antlr.PythonParser.and_expr_return;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;

/**
 *
 *
/**
 * Input Adapter that scans for files in the specified local directory.
 * Since the operator can be deployed anywhere in the cluster the directory 
 * to be scanned should be available on all nodes of the cluster.
 * Not to be used for HDFS. 
 * <p>
 */
public class DirectoryScanInputOperator extends BaseOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private File baseDirectory = null;

  @NotNull
  private String directoryPath;//the directory that is scanned
  private int scanIntervalInMilliSeconds = 1000;// the time interval for periodically scanning, Default is 1 sec = 1000ms
  private int fileCountPerEmit = 200; // the maximum number of file info records that will be output per emit

  public final transient DefaultOutputPort<FileInfoRecord> outport = new DefaultOutputPort<FileInfoRecord>();
  private transient FileAlterationObserver observer;
  private transient FAListener listener;

  private LinkedBlockingQueue<FileInfoRecord> fileRecordsQueue = new LinkedBlockingQueue<FileInfoRecord>();
  
  private transient Thread directoryScanThread = new Thread(new DirectoryScanner());

  public class FAListener implements FileAlterationListener
  {
    // Is triggered when a file is created in the monitored folder
    
    @Override    
    public void onFileCreate(File file)
    {
      FileInfoRecord fileRecord = new FileInfoRecord(file.getAbsolutePath(), FilenameUtils.getExtension(file.getAbsolutePath()), file.length());
      // fileRecordsQueue.add(fileRecord);
      try {
        fileRecordsQueue.put(fileRecord);
      } catch (InterruptedException e) {
        DTThrowable.rethrow(e);
      }
    }
    
    
    @Override
    public void onFileDelete(File file)
    {
      FileInfoRecord fileToDelete = new FileInfoRecord(file.getAbsolutePath(), FilenameUtils.getExtension(file.getAbsolutePath()), file.length());
      fileRecordsQueue.remove(fileToDelete);
    }

    @Override
    public void onDirectoryCreate(File directory)
    {
      //files within the directory will in turn generate onFileCreate events
    }

    @Override
    public void onDirectoryDelete(File directory)
    {
     //files within the directory will in turn generate onFileDelete events
    }

    @Override
    public void onDirectoryChange(File directory)
    {
    }

    @Override
    public void onFileChange(File file)
    {
    }

    @Override
    public void onStart(FileAlterationObserver arg0)
    {
    }

    @Override
    public void onStop(FileAlterationObserver arg0)
    {
    }
  }

  /**
   * Thread that periodically scans the directory.
   * 
   */
  public class DirectoryScanner implements Runnable
  {
    public void run()
    {
      try {
        while (true) {
          observer.checkAndNotify();
          // Sleep for Scan interval
          Thread.sleep(scanIntervalInMilliSeconds);
        }
      } catch (InterruptedException e) {
         return;
      }
    }
  }

  /**
   * Sets the path of the directory to be scanned. 
   * 
   * @param dirPath
   *          the path of the directory to be scanned
   */
  public void setDirectoryPath(String dirPath)
  {
    this.directoryPath = dirPath;
  }

  /**
   * Returns the path of the directory being scanned.
   * 
   * @return directoryPath the path of directory being scanned.
   */
  public String getDirectoryPath()
  {
    return directoryPath;
  }

  /**
   * Sets the time interval at which the directory is to be scanned
   * 
   * @param scanIntervalInMilliSeconds
   *          the time interval for scanning the directory
   */
  public void setScanIntervalInMilliSeconds(int scanIntervalInMilliSeconds)
  {
    this.scanIntervalInMilliSeconds = scanIntervalInMilliSeconds;
  }

  /**
   * Returns the interval at which the directory is being scanned.
   * 
   * @return scanIntervalInMilliSeconds the interval at which the directory is being scanned
   */
  public int getScanIntervalInMilliSeconds()
  {
    return scanIntervalInMilliSeconds;
  }

  /**
   * Sets the number of file records to output in one emit cycle
   * 
   * @param fileCount
   *          the number of file records to output per emit
   */
  public void setFileCountPerEmit(int fileCount)
  {
    this.fileCountPerEmit = fileCount;
  }

  /**
   * Returns the number of file records that are output per emit.
   * 
   * @return fileCountPerEmit the number of file records that are output per emit
   */
  public int getFileCountPerEmit()
  {
    return fileCountPerEmit;
  }

  /**
   * Creates the directory instance for the specified directory path and 
   * check if it exists.
   * 
   */
  @Override
  public void setup(OperatorContext context)
  {

  }

  /**
   * Initializes the directory scan monitoring mechanism 
   * and starts the thread that periodically scans the directory.
   */
  @Override
  final public void activate(OperatorContext ctx)
  {    
     baseDirectory = new File(directoryPath.replace('/', File.separatorChar).replace('\\', File.separatorChar));
  
     if (!baseDirectory.exists()) {
       // Check if monitored folder exists      
       throw new RuntimeException("Directory not found: " + baseDirectory);
     }
    
    // Start the directory monitor
    observer = new FileAlterationObserver(baseDirectory);

    listener = new FAListener();
    observer.addListener(listener);

    directoryScanThread.start();
    
  }

  /**
   * Stops the thread that periodically scans the directory
   */
  @Override
  final public void deactivate()
  {
    // Interrupt the directoryScanThread and wait for it to join
    directoryScanThread.interrupt();
    try {
      directoryScanThread.join();
    } catch (InterruptedException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void teardown()
  {

  }

  /**
   * Emits the file info records up-to a maximum of the specified number of file records per emit. Each file info record
   * has the file name, file type {@link and_expr_return} file size.
   */
  @Override
  public void emitTuples()
  {    
    int count = fileCountPerEmit;
    try {
      while (!fileRecordsQueue.isEmpty() && count > 0) {
        outport.emit(fileRecordsQueue.take());
        count--;
      }
    } catch (InterruptedException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {
  }

}
