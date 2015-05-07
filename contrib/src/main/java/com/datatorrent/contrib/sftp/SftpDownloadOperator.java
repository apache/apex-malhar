/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.sftp;

import java.io.*;
import java.io.IOException;
import java.util.*;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.google.common.collect.Lists;
import com.google.common.io.CountingOutputStream;
import com.jcraft.jsch.*;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import static com.jcraft.jsch.Logger.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.common.util.DTThrowable;

/**
 * This operator receives the remote absolute file path in the input port and downloads it to the specified fs path in application directory.
 *
 * Once the file is downloaded, the absolute file path of the downloaded file is emitted though the output port.
 *
 * This operator currently works with the following types of authentication
 *  publickey
 *  password
 *  keybaord-interactive
 *
 * This operator is functional but required more testing to call it complete. So it still work in progress.
 *
 */
public class SftpDownloadOperator extends BaseOperator implements Operator.IdleTimeHandler, Operator.ActivationListener<Context.OperatorContext>
{
  private static final String TMP = ".tmp";
  //Ftp properties
  @NotNull
  private String ftpHost;
  @Min(0)
  private int ftpPort;
  private final transient AtomicReference<Throwable> throwable;
  public String knownHosts;
  public String passphrase;

  public String getPassphrase()
  {
    return passphrase;
  }

  public void setPassphrase(String passphrase)
  {
    this.passphrase = passphrase;
  }

  @NotNull
  private String userName;
  private String password;
  private String passwordFile;
  @NotNull
  private String downloadDirectory;
  private transient FileSystem fileSystem;
  private transient String appDir;
  private List<String> waiting;
  private transient BlockingQueue<String> doneFiles;
  private transient BlockingQueue<String> outputPath;
  private transient FtpDownloadService ftpDownloadService;
  private transient long sleepTimeMillis;
  private String privateKey;
  private byte[] privateKeyBytes;
  private boolean keyboardInteractive;

  public boolean isKeyboardInteractive()
  {
    return keyboardInteractive;
  }

  public void setKeyboardInteractive(boolean keyboardInteractive)
  {
    this.keyboardInteractive = keyboardInteractive;
  }

  public String getPrivateKey()
  {
    return privateKey;
  }

  public void setPrivateKey(String privateKey)
  {
    this.privateKey = privateKey;
    if (privateKey != null) {
      privateKeyBytes = getPrivateKeyAsByteStream();
    }
  }

  public SftpDownloadOperator()
  {
    waiting = Lists.newArrayList();
    doneFiles = new LinkedBlockingQueue<String>();
    outputPath = new LinkedBlockingQueue<String>();
    throwable = new AtomicReference<Throwable>();
  }

  /**
   *
   * This method is mainly helpful for unit testing.
   *
   * @param filePath
   * @return A FileSystem object.
   * @throws IOException
   */
  protected FileSystem getFSInstance(String filePath) throws IOException
  {
    FileSystem tempFS = FileSystem.newInstance(new Path(filePath).toUri(), new Configuration());

    if (tempFS instanceof LocalFileSystem) {
      tempFS = ((LocalFileSystem)tempFS).getRaw();
    }

    return tempFS;
  }

  @Override
  public void setup(OperatorContext context)
  {
    appDir = downloadDirectory + '/' + context.getValue(DAG.APPLICATION_ID);
    try {
      fileSystem = getFSInstance(appDir);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    logger.debug("password file = {}", passwordFile);
    if (passwordFile != null) {
      try {
        Path path = new Path(passwordFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        String line = bufferedReader.readLine();
        password = line.trim();
      }
      catch (IOException e) {
        DTThrowable.rethrow(e);
      }
      logger.debug("password is {}", password);
    }

    sleepTimeMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    //setup ftp client
    ftpDownloadService = new FtpDownloadService();
  }

  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String fileInfo)
    {
      waiting.add(fileInfo);
      ftpDownloadService.eventQueue.add(fileInfo);
    }

  };
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  public void teardown()
  {
    try {
      ftpDownloadService.stopService();
      fileSystem.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void activate(Context.OperatorContext operatorContext)
  {
    if (waiting.size() > 0) {
      Iterator<String> itr = waiting.iterator();
      while (itr.hasNext()) {
        String fileInfo = itr.next();
        int idx = fileInfo.lastIndexOf("/");
        String fileName = fileInfo.substring(idx + 1, fileInfo.length());
        String tempFileName = fileName + TMP;
        Path tempPath = new Path(appDir, tempFileName);
        try {
          if (fileSystem.exists(tempPath)) {
            fileSystem.delete(tempPath, false);
          }
          if (!fileSystem.exists(new Path(appDir, fileName))) {
            ftpDownloadService.eventQueue.add(fileInfo);
          }
        }
        catch (IOException ex) {
          DTThrowable.rethrow(ex);
        }
      }
    }
  }

  private byte[] getPrivateKeyAsByteStream()
  {
    File privateKeyLocation = new File(privateKey);
    InputStream is = null;
    try {
      is = new FileInputStream(privateKeyLocation);
    }
    catch (FileNotFoundException e) {
      DTThrowable.rethrow(e);
    }
    long length = privateKeyLocation.length();
    if (length > Integer.MAX_VALUE) {
      try {
        throw new IOException(
                "File to process is too big to process in this example.");
      }
      catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }

    final byte[] bytes = new byte[(int)length];

    // Read in the bytes
    int offset = 0;
    int numRead = 0;
    try {
      if (is != null) {
        while ((offset < bytes.length)
                && ((numRead = is.read(bytes, offset, bytes.length - offset)) >= 0)) {

          offset += numRead;

        }
      }
    }
    catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    // Ensure all the bytes have been read in
    if (offset < bytes.length) {
      try {
        throw new IOException("Could not completely read file "
                + privateKeyLocation.getName());
      }
      catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }

    try {
      if (is != null) {
        is.close();
      }
    }
    catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    return bytes;

  }

  protected void completed(String fileInfo, Path path)
  {
    doneFiles.add(fileInfo);
    if (output.isConnected()) {
      outputPath.add(path.toString());
    }
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void handleIdleTime()
  {
    if (doneFiles.isEmpty() && throwable.get() == null) {
      /* nothing to do here, so sleep for a while to avoid busy loop */
      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    else if (throwable.get() != null) {
      DTThrowable.rethrow(throwable.get());
    }
    else {
      /**
       * Remove all the events from waiting which have been ftp-ed
       */
      String fileInfo;
      while ((fileInfo = doneFiles.poll()) != null) {
        waiting.remove(fileInfo);
        //changing file-path to download file-path

        if (output.isConnected()) {
          output.emit(outputPath.poll());
        }
      }
    }
  }

  private class FtpDownloadService implements Runnable
  {
    private transient volatile boolean running;
    private transient final BlockingQueue<String> eventQueue;
    ChannelSftp channelSftp = null;
    Session session = null;

    FtpDownloadService()
    {
      eventQueue = new LinkedBlockingQueue<String>();
      Thread eventServiceThread = new Thread(this, "FtpService");
      eventServiceThread.start();
    }

    private void connect() throws Exception
    {
      logger.info("connecting... username = {} private key file = {} private key bytes = {} password = {} keyboardInteractive = {}", userName, privateKey, privateKeyBytes, password, keyboardInteractive);
      JSch.setLogger(new JschLogger());
      JSch jsch = new JSch();
      if (privateKey != null) {
        jsch.addIdentity(userName, privateKeyBytes, null, new byte[0]);
        session = jsch.getSession(userName, ftpHost, ftpPort);
        logger.debug("identity added");
      }
      else {
        session = jsch.getSession(userName, ftpHost, ftpPort);
        if (keyboardInteractive) {
          logger.debug("auth method is keyboardInteractive");
          MyUserInfo ui = new MyUserInfo();
          ui.passwd = password;
          session.setUserInfo(ui);
        }
        else {
          logger.debug("auth method is password");
          session.setPassword(password);
        }
      }

      logger.debug("username = {} host = {} port ={}", userName, ftpHost, ftpPort);

      Properties config = new Properties();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      logger.debug("session trying to connect...");
      session.connect();
      logger.debug("session connected...");
      Channel channel = session.openChannel("sftp");
      channel.connect();
      logger.debug("channel connected...");
      channelSftp = (ChannelSftp)channel;
    }

    private void startFTP(String fileInfo) throws Exception
    {
      String tempFileName = null;
      try {
        int idx = fileInfo.lastIndexOf("/");
        String fileName = fileInfo.substring(idx + 1, fileInfo.length());
        Path path = new Path(appDir, fileName);

        tempFileName = fileName + TMP;
        Path tempPath = new Path(appDir, tempFileName);
        if (session == null || !session.isConnected()) {
          connect();
        }
        logger.debug("reading file {}", fileInfo);
        Vector ls = channelSftp.ls(fileInfo);
        if (!ls.isEmpty()) {
          CountingOutputStream outputStream = new CountingOutputStream(fileSystem.create(tempPath, true));
          channelSftp.get(fileInfo, outputStream);
          logger.debug("File download successful");
          outputStream.close();
          logger.debug("file closed");
          FileContext fileContext = FileContext.getFileContext(fileSystem.getUri());
          fileContext.rename(tempPath, path, Options.Rename.OVERWRITE);
          completed(fileInfo, path);
          //}
        }
        else {
          logger.error("{} file not found in sftp location", fileInfo);
        }
      }
      catch (Exception ex) {
        logger.error("exception while reading file from ftp location {} to write to file {}", fileInfo, tempFileName);
        logger.error("Exception: ", ex);
      }

    }

    @Override
    public void run()
    {
      running = true;
      try {
        while (running) {
          String fileInfo = eventQueue.poll(1, TimeUnit.SECONDS);
          if (fileInfo != null) {
            startFTP(fileInfo);
          }
        }
      }
      catch (Throwable cause) {
        running = false;
        throwable.set(cause);
      }
    }

    private void stopService() throws IOException
    {
      running = false;
      //disconnect
      if (session != null && session.isConnected()) {
        channelSftp.exit();
        session.disconnect();
      }
    }

  }

  public static class MyUserInfo implements UserInfo, UIKeyboardInteractive
  {
    public String passwd;

    @Override
    public String getPassphrase()
    {
      return null;
    }

    @Override
    public String getPassword()
    {
      return passwd;
    }

    @Override
    public boolean promptPassword(String message)
    {
      return false;
    }

    @Override
    public boolean promptPassphrase(String message)
    {
      return false;
    }

    @Override
    public boolean promptYesNo(String message)
    {
      return true;
    }

    @Override
    public void showMessage(String message)
    {
    }

    @Override
    public String[] promptKeyboardInteractive(String destination,
            String name,
            String instruction,
            String[] prompt,
            boolean[] echo)
    {
      logger.debug("destination: " + destination);
      logger.debug("name: " + name);
      logger.debug("instruction: " + instruction);
      logger.debug("prompt.length: " + prompt.length);
      logger.debug("prompt: " + prompt[0]);
      String[] response = new String[prompt.length];

      for (int i = 0; i < prompt.length; i++) {
        response[i] = passwd;
      }
      return response;
    }

  }

  public String getFtpHost()
  {
    return ftpHost;
  }

  public void setFtpHost(String ftpHost)
  {
    this.ftpHost = ftpHost;
  }

  public int getFtpPort()
  {
    return ftpPort;
  }

  public void setFtpPort(int ftpPort)
  {
    this.ftpPort = ftpPort;
  }

  public String getUserName()
  {
    return userName;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getPassword()
  {
    return password;
  }

  public void setPassword(String password)
  {
    this.password = password;
  }

  public String getDownloadDirectory()
  {
    return downloadDirectory;
  }

  public void setDownloadDirectory(String downloadDirectory)
  {
    this.downloadDirectory = downloadDirectory;
  }

  public String getPasswordFile()
  {
    return passwordFile;
  }

  public void setPasswordFile(String passwordFile)
  {
    this.passwordFile = passwordFile;
  }

  private class JschLogger implements com.jcraft.jsch.Logger
  {
    private Map<Integer, Level> levels = new HashMap<Integer, Level>();
    private final Logger jschLogger;

    public JschLogger()
    {
      // Mapping between JSch levels and our own levels
      levels.put(DEBUG, Level.DEBUG);
      levels.put(INFO, Level.INFO);
      levels.put(WARN, Level.WARN);
      levels.put(ERROR, Level.ERROR);
      levels.put(FATAL, Level.FATAL);

      jschLogger = LoggerFactory.getLogger(JschLogger.class);
    }

    @Override
    public boolean isEnabled(int pLevel)
    {
      return true; // here, all levels enabled
    }

    @Override
    public void log(int pLevel, String txt)
    {
      Level level = levels.get(pLevel);
      if (level == null) {
        level = Level.DEBUG;
      }
      switch (level.toInt()) {
        case Level.TRACE_INT:
          jschLogger.trace(txt);
          break;
        case Level.DEBUG_INT:
          jschLogger.debug(txt);
          break;
        case Level.INFO_INT:
          jschLogger.info(txt);
          break;
        case Level.WARN_INT:
          jschLogger.warn(txt);
          break;
        case Level.ERROR_INT:
          jschLogger.error(txt);
          break;
      }
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(SftpDownloadOperator.class);
}
