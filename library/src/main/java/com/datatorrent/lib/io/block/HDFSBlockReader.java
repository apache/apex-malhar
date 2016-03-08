package com.datatorrent.lib.io.block;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Splitter;

public class HDFSBlockReader extends FSSliceReader
{
  protected String uri;

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    return FileSystem.newInstance(URI.create(uri), configuration);
  }

  /**
   * Sets the uri
   *
   * @param uri
   */
  public void setUri(String uri)
  {
    this.uri = convertSchemeToLowerCase(uri);
  }

  public String getUri()
  {
    return uri;
  }

  /**
   * Converts Scheme part of the URI to lower case. Multiple URI can be comma separated. If no scheme is there, no
   * change is made.
   * 
   * @param
   * @return String with scheme part as lower case
   */
  private static String convertSchemeToLowerCase(String uri)
  {
    if (uri == null)
      return null;
    StringBuilder inputMod = new StringBuilder();
    for (String f : Splitter.on(",").omitEmptyStrings().split(uri)) {
      String scheme = URI.create(f).getScheme();
      if (scheme != null) {
        inputMod.append(f.replaceFirst(scheme, scheme.toLowerCase()));
      } else {
        inputMod.append(f);
      }
      inputMod.append(",");
    }
    inputMod.setLength(inputMod.length() - 1);
    return inputMod.toString();
  }
}
