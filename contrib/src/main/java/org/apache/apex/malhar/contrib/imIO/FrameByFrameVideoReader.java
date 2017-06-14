/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.apex.malhar.contrib.imIO;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.imageio.ImageIO;
import org.opencv.core.Mat;
import org.opencv.videoio.VideoCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import static org.opencv.videoio.Videoio.CAP_PROP_FPS;
import static org.opencv.videoio.Videoio.CAP_PROP_FRAME_COUNT;
import static org.opencv.videoio.Videoio.CV_CAP_FFMPEG;


public class FrameByFrameVideoReader extends FileReaderA
{
  private static final Logger LOG = LoggerFactory.getLogger(FrameByFrameVideoReader.class);
  imIOHelper imIOHelper = new imIOHelper();
  protected String soPath = imIOHelper.soPath;
  private int framesInAVideo;
  private int framesPerSecond;
  private boolean webCam;
  private int seconds;
  private int currentFrame;
  private transient VideoCapture videoCapture;
  private transient Path filePath;

  public boolean isWebCam()
  {
    return webCam;
  }

  public void setWebCam(boolean webCam)
  {
    this.webCam = webCam;
  }

  public int getSeconds()
  {
    return seconds;
  }

  public void setSeconds(int seconds)
  {
    this.seconds = seconds;
  }

  @Override
  protected InputStream openFile(Path curPath) throws IOException
  {
    System.load(soPath);
    LOG.debug("openFile: curPath = {}", curPath);
    filePath = curPath;
    filePathStr = filePath.toString();
    InputStream is = super.openFile(filePath);
    if (currentFrame == 0) {
      videoCapture = new VideoCapture(filePathStr, CV_CAP_FFMPEG);
      if (isWebCam()) {
        videoCapture = new VideoCapture(0);
      }
      framesPerSecond = (int)Math.ceil(videoCapture.get(CAP_PROP_FPS));
      //framesPerSecond = framesPerSecond/10;
      if (!isWebCam()) {
        framesInAVideo = (int)Math.ceil(videoCapture.get(CAP_PROP_FRAME_COUNT));
      }

    }
    return is;
  }

  @Override
  protected Data readEntity() throws IOException
  {
    if (currentFrame < framesInAVideo || isWebCam()) {
      System.load(soPath);
      Mat frame = new Mat();
      videoCapture.read(frame);
      Data data = new Data();
      data.bytesImage = writeMat(frame);
      data.fileName = filePath.getName() + "_" + currentFrame;
      if (isWebCam()) {
        data.fileName = Integer.toString(currentFrame) + ".png";
      }
      return data;
    }
    return null;
  }

  protected byte[] writeMat(Mat write)
  {
    System.load(soPath);
    int bufferedImageType = 0;
    ByteArrayOutputStream byteArrayOutputStream = null;
    if (write.type() == 16) {
      bufferedImageType = BufferedImage.TYPE_3BYTE_BGR;
    }
    BufferedImage bufferedImage1;
    bufferedImage1 = new BufferedImage(write.width(), write.height(), bufferedImageType);
    //bufferedImage1 = convertToRGB(bufferedImage1);
    byte[] data = new byte[((int)write.total() * write.channels())];
    write.get(0, 0, data);
    //byte[] data = matOfByte.toArray();
    byte b;
    for (int i = 0; i < data.length; i = i + 3) {
      b = data[i];
      data[i] = data[i + 2];
      data[i + 2] = b;
    }
    try {
      bufferedImage1.getRaster().setDataElements(0, 0, write.cols(), write.rows(), data);
      byteArrayOutputStream = new ByteArrayOutputStream();
      ImageIO.write(bufferedImage1, "jpg", byteArrayOutputStream);
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }
    return byteArrayOutputStream.toByteArray();

  }

  @Override
  protected void emit(Data data)
  {
    LOG.info("FPS: " + framesPerSecond + " seconds: " + seconds + " currentFrame " +
        currentFrame + " totalFrames " + framesInAVideo);
    int frameCheck = 0;
    if (seconds != 0) {
      frameCheck = currentFrame % (framesPerSecond * seconds);
    }
    if (isWebCam()) {
      currentFrame++;
    }
    if (!isWebCam()) {
      if (currentFrame <= framesInAVideo) {
        currentFrame++;
      } else {
        currentFrame = 0;
      }
    }
    if (frameCheck == 0) {
      output.emit(data);
      LOG.info("sent data from read");
    }

  }
}
