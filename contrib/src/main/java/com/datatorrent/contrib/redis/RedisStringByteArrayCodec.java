/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.contrib.redis;

import static java.nio.charset.CoderResult.OVERFLOW;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.lambdaworks.redis.codec.RedisCodec;

/**
 * Codec to convert keys to UTF-8 strings, and leave messages bytes unchanged. 
 *
 */
class RedisStringByteArrayCodec extends RedisCodec<String, byte[]>
{

  private Charset charset;
  private CharsetDecoder decoder;
  private CharBuffer chars;

  public RedisStringByteArrayCodec()
  {
    charset = Charset.forName("UTF-8");
    decoder = charset.newDecoder();
    chars = CharBuffer.allocate(1024);
  }

  @Override
  public String decodeKey(ByteBuffer bytes)
  {
    return decodeString(bytes);
  }

  @Override
  public byte[] decodeValue(ByteBuffer bytes)
  {
    byte[] b = new byte[bytes.remaining()];
    bytes.get(b);
    return b;
  }

  @Override
  public byte[] encodeKey(String key)
  {
    return encodeString(key);
  }

  @Override
  public byte[] encodeValue(byte[] value)
  {
    return value;
  }

  private String decodeString(ByteBuffer bytes)
  {
    chars.clear();
    bytes.mark();

    decoder.reset();
    while (decoder.decode(bytes, chars, true) == OVERFLOW || decoder.flush(chars) == OVERFLOW) {
      chars = CharBuffer.allocate(chars.capacity() * 2);
      bytes.reset();
    }

    return chars.flip().toString();
  }

  private byte[] encodeString(String string)
  {
    return string.getBytes(charset);
  }

};
