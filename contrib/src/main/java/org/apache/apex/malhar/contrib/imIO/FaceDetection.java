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

import javax.validation.constraints.NotNull;

import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.objdetect.CascadeClassifier;
import com.datatorrent.api.DefaultOutputPort;


import static org.opencv.imgproc.Imgproc.rectangle;

public class FaceDetection extends imIOHelper
{
  public final transient DefaultOutputPort<Data> template = new DefaultOutputPort<>();
  @NotNull
  private String xmlPath;

  public String getXmlPath()
  {
    return xmlPath;
  }

  public void setXmlPath(String xmlPath)
  {
    this.xmlPath = xmlPath;
  }

  private void detect(Data data)
  {
    Mat image = readMat(data.bytesImage);
    CascadeClassifier cascadeClassifier = new CascadeClassifier(xmlPath);
    //int f=0;
    //for(Mat image:matFrames)
    MatOfRect faceDetections = new MatOfRect();
    cascadeClassifier.detectMultiScale(image, faceDetections);
    int rN = 0;
    if (faceDetections.toArray().length >= 1) {
      for (int j = 0; j < 1; j++) {
        Rect rect = faceDetections.toArray()[faceDetections.toArray().length - 1];
        rN++;
        Rect rect1 = new Rect(rect.x, rect.y, rect.width, rect.height);
        Mat r = image.submat(rect1);
        if (r.width() > 180) {
          rectangle(image, new Point(rect.x, rect.y), new Point(rect.x + rect.width, rect.y + rect.height),
              new Scalar(0, 255, 0));
          Data data1 = new Data();
          data1.bytesImage = writeMat(r);
          data1.fileName = data.fileName;
          template.emit(data1);
        }
      }
    }
    data.bytesImage = writeMat(image);
    data.fileName = data.fileName + ".png";
    output.emit(data);
  }

  @Override
  void processTuple(Data data)
  {
    detect(data);
  }
}
