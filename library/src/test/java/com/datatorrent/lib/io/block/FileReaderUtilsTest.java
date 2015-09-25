package com.datatorrent.lib.io.block;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Assert;
import org.junit.Test;

public class FileReaderUtilsTest
{

  @Test
  public void testUpperCaseSchemeToLowerCase() throws URISyntaxException
  {
    String scheme = "HDFS";
    String host = "mynode.cluster.com:8020";
    String path = "/user/MyUser/input";
    URI uri = new URI(scheme + "://" + host + path);
    String processedUriStr = FileReaderUtils.convertSchemeToLowerCase(uri.toString());
    URI processedUri = new URI(processedUriStr);

    Assert.assertEquals(scheme.toLowerCase(), processedUri.getScheme());
    Assert.assertEquals(path, processedUri.getPath());
  }

  @Test
  public void testMixedCaseSchemetoLawerCase() throws URISyntaxException
  {
    String scheme = "Ftp";
    String host = "mynode.cluster.com:8020";
    String path = "/user/MyUser/INPUT";
    URI uri = new URI(scheme + "://" + host + path);
    String processedUriStr = FileReaderUtils.convertSchemeToLowerCase(uri.toString());
    URI processedUri = new URI(processedUriStr);

    Assert.assertEquals(scheme.toLowerCase(), processedUri.getScheme());
    Assert.assertEquals(path, processedUri.getPath());
  }
}
