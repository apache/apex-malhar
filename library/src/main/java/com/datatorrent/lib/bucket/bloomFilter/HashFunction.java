package com.datatorrent.lib.bucket.bloomFilter;

import com.sangupta.murmur.Murmur3;

/**
 * Hash function returns a single long hash value.
 *
 */
class HashFunction
{
  private static final long SEED = 0x7f3a21eal;

  /**
   * Return the hash of the bytes as long.
   *
   * @param bytes
   *          the bytes to be hashed
   *
   * @return the generated hash value
   */
  public long hash(byte[] bytes)
  {
    return Murmur3.hash_x64_128(bytes, 0, SEED)[0];
  }
}
