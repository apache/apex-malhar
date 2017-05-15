package org.apache.apex.malhar.contrib.imIO;
/*
 * imIO4
 * Created by Aditya Gholba on 20/2/17.
 */

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import javax.imageio.ImageIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.coobird.thumbnailator.Thumbnails;




public class Resize extends ToolKit
{
  private static final Logger LOG = LoggerFactory.getLogger(Resize.class);
  protected int width = 0;
  protected int height = 0;
  protected double scale = 1;

  public double getScale()
  {
    return scale;
  }

  public void setScale(double scale)
  {
    this.scale = scale;
  }

  public int getWidth()
  {
    return width;
  }

  public void setWidth(int width)
  {
    this.width = width;
  }

  public int getHeight()
  {
    return height;
  }

  public void setHeight(int height)
  {
    this.height = height;
  }

  private void resize(Data data)
  {
    try {
      byte[] byteImage = data.bytesImage;
      InputStream in = new ByteArrayInputStream(byteImage);
      bufferedImage = ImageIO.read(in);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      BufferedImage resizedImage;
      LOG.info("fileTypeIs:" + ToolKit.fileType + " scale:" + scale);
      //BufferedImage resizedImage = Thumbnails.of(bufferedImage).size(width, height).asBufferedImage();
      if (height == width && width == 0) {
        resizedImage = Thumbnails.of(bufferedImage).scale(scale).asBufferedImage();
      } else {
        resizedImage = Thumbnails.of(bufferedImage).size(width, height).asBufferedImage();
      }
      ImageIO.write(resizedImage, ToolKit.fileType, baos);
      data.bytesImage = baos.toByteArray();
      output.emit(data);
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }
  }

  @Override
  void processTuple(Data data)
  {
    resize(data);
  }

}
