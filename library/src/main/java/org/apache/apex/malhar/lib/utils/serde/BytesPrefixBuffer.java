package org.apache.apex.malhar.lib.utils.serde;

/**
 * Unlike LengthValueBuffer which start with the length, 
 * This serialize buffer prefix with a fixed byte array.
 * This SerializeBuffer can be used for key which prefixed by identifier 
 *
 */
public class BytesPrefixBuffer extends AbstractSerializeBuffer
{
  protected byte[] prefix;
  
  public BytesPrefixBuffer()
  {
    windowableByteStream = createWindowableByteStream();
  }

  public BytesPrefixBuffer(byte[] prefix)
  {
    setPrefix(prefix);
    windowableByteStream = createWindowableByteStream();
  }
      
  public BytesPrefixBuffer(byte[] prefix, int capacity)
  {
    windowableByteStream = createWindowableByteStream(capacity);
  }
  
  /**
   * set value and length. the input value is value only, it doesn't include
   * length information.
   * 
   * @param value
   * @param offset
   * @param length
   */
  @Override
  public void setObjectByValue(byte[] value, int offset, int length)
  {
    if (prefix != null && prefix.length > 0) {
      write(prefix);
    }
    write(value, offset, length);
  }

  public byte[] getPrefix()
  {
    return prefix;
  }

  public void setPrefix(byte[] prefix)
  {
    this.prefix = prefix;
  }
  
}
