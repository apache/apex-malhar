package org.apache.apex.malhar.contrib.imIO;
/*
 * imIO4
 * Created by Aditya Gholba on 20/2/17.
 */
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Iterator;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Compress extends ToolKit
{
  private static final Logger LOG = LoggerFactory.getLogger(Compress.class);
  private float compressionRatio;

  public float getCompressionRatio()
  {
    return compressionRatio;
  }

  public void setCompressionRatio(float compressionRatio)
  {
    this.compressionRatio = compressionRatio;
  }

  private void compress(Data data)
  {
    LOG.info("rec data");
    if (data != null) {
      try {
        byte[] byteImage = data.bytesImage;
        InputStream in = new ByteArrayInputStream(byteImage);
        bufferedImage = ImageIO.read(in);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName(ToolKit.fileType);
        ImageWriter writer = writers.next();
        writer.setOutput(new MemoryCacheImageOutputStream(baos));

        ImageWriteParam param = writer.getDefaultWriteParam();
        if (param.canWriteCompressed()) {
          param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
          param.setCompressionQuality(compressionRatio);
        }
        writer.write(null, new IIOImage(bufferedImage, null, null), param);
        writer.dispose();
        data.bytesImage = baos.toByteArray();
        baos.flush();
        baos.reset();
        baos.close();
        output.emit(data);
        LOG.info("send data from compress");
      } catch (Exception e) {
        LOG.info("compressError " + e.getMessage());
      }
    }
  }

  @Override
  void processTuple(Data data)
  {
    compress(data);
  }
}
