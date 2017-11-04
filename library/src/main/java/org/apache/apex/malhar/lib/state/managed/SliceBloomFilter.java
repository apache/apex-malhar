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
 */

/**
 *
 * bloomfilter - Bloom filters for Java
 * Copyright (c) 2014-2015, Sandeep Gupta
 *
 * http://sangupta.com/projects/bloomfilter
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
 *
 */

/**
 *
 * murmurhash - Pure Java implementation of the Murmur Hash algorithms.
 * Copyright (c) 2014, Sandeep Gupta
 *
 * http://sangupta.com/projects/murmur
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
 *
 */

package org.apache.apex.malhar.lib.state.managed;

import java.util.BitSet;

import com.datatorrent.netlet.util.Slice;

/**
 * This class implemented BloomFilter algorithm, the key is Slice
 * Bloom filter and murmur hash implementations originally from Sandeep Gupta
 * <a href="https://github.com/sangupta/bloomfilter/blob/master/src/main/java/com/sangupta/bloomfilter/BloomFilter.java">BloomFilter.java</a>
 * <a href="https://github.com/sangupta/murmur/blob/master/src/main/java/com/sangupta/murmur/Murmur3.java">Murmur3.java</a>
 *
 *
 * @since 3.8.0
 */
public class SliceBloomFilter
{
  private BitSet bitset;
  private int bitSetSize;
  private int expectedNumberOfFilterElements; // expected (maximum) number of elements to be added
  private int numberOfAddedElements; // number of elements actually added to the Bloom filter
  private int numberOfHashes; // number of hash functions
  protected transient HashFunction hasher = new HashFunction();

  /**
   * Set the attributes to the empty Bloom filter. The total length of the Bloom
   * filter will be bitsPerElement*expectedNumberOfFilterElements.
   *
   * @param bitsPerElement
   *          is the number of bits used per element.
   * @param expectedNumberOfFilterElements
   *          is the expected number of elements the filter will contain.
   * @param numberOfHashes
   *          is the number of hash functions used.
   */
  private void SetAttributes(double bitsPerElement, int expectedNumberOfFilterElements, int numberOfHashes)
  {
    this.expectedNumberOfFilterElements = expectedNumberOfFilterElements;
    this.numberOfHashes = numberOfHashes;
    this.bitSetSize = (int)Math.ceil(bitsPerElement * expectedNumberOfFilterElements);
    numberOfAddedElements = 0;
    this.bitset = new BitSet(bitSetSize);
  }

  private SliceBloomFilter()
  {
    //for kyro
  }

  /**
   * Constructs an empty Bloom filter with a given false positive probability.
   *
   * @param expectedNumberOfElements
   *          is the expected number of elements the filter will contain.
   * @param falsePositiveProbability
   *          is the desired false positive probability.
   */
  public SliceBloomFilter(int expectedNumberOfElements, double falsePositiveProbability)
  {
    if (this.bitset == null) {
      SetAttributes(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2), // c = k / ln(2)
          expectedNumberOfElements, (int)Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))));
    }
  }

  /**
   * Generate integer array based on the hash function till the number of
   * hashes.
   *
   * @param slice
   *          specifies input slice.
   * @return array of int-sized hashes
   */
  private int[] createHashes(Slice slice)
  {
    int[] result = new int[numberOfHashes];
    long hash64 = hasher.hash(slice);
    // apply the less hashing technique
    int hash1 = (int)hash64;
    int hash2 = (int)(hash64 >>> 32);
    for (int i = 1; i <= numberOfHashes; i++) {
      int nextHash = hash1 + i * hash2;
      if (nextHash < 0) {
        nextHash = ~nextHash;
      }
      result[i - 1] = nextHash;
    }
    return result;
  }

  /**
   * Calculates the expected probability of false positives based on the number
   * of expected filter elements and the size of the Bloom filter.
   *
   * @return expected probability of false positives.
   */
  public double expectedFalsePositiveProbability()
  {
    return getFalsePositiveProbability(expectedNumberOfFilterElements);
  }

  /**
   * Calculate the probability of a false positive given the specified number of
   * inserted elements.
   *
   * @param numberOfElements
   *          number of inserted elements.
   * @return probability of a false positive.
   */
  public double getFalsePositiveProbability(double numberOfElements)
  {
    // (1 - e^(-k * n / m)) ^ k
    return Math.pow((1 - Math.exp(-numberOfHashes * numberOfElements / bitSetSize)), numberOfHashes);
  }

  /**
   * Get the current probability of a false positive. The probability is
   * calculated from the size of the Bloom filter and the current number of
   * elements added to it.
   *
   * @return probability of false positives.
   */
  public double getFalsePositiveProbability()
  {
    return getFalsePositiveProbability(numberOfAddedElements);
  }

  /**
   * Returns the value chosen for numberOfHashes.<br />
   * <br />
   * numberOfHashes is the optimal number of hash functions based on the size of
   * the Bloom filter and the expected number of inserted elements.
   *
   * @return optimal numberOfHashes.
   */
  public int getNumberOfHashes()
  {
    return numberOfHashes;
  }

  /**
   * Sets all bits to false in the Bloom filter.
   */
  public void clear()
  {
    bitset.clear();
    numberOfAddedElements = 0;
  }

  /**
   * Adds a slice of byte array to the Bloom filter.
   *
   * @param slice
   *          slice of byte array to add to the Bloom filter.
   */
  public void put(Slice slice)
  {
    int[] hashes = createHashes(slice);
    for (int hash : hashes) {
      bitset.set(Math.abs(hash % bitSetSize), true);
    }
    numberOfAddedElements++;
  }

  /**
   * Returns true if the slice of byte array could have been inserted into the Bloom
   * filter. Use getFalsePositiveProbability() to calculate the probability of
   * this being correct.
   *
   * @param slice
   *          slice of byte array to check.
   * @return true if the array could have been inserted into the Bloom filter.
   */
  public boolean mightContain(Slice slice)
  {
    int[] hashes = createHashes(slice);
    for (int hash : hashes) {
      if (!bitset.get(Math.abs(hash % bitSetSize))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Read a single bit from the Bloom filter.
   *
   * @param bit
   *          the bit to read.
   * @return true if the bit is set, false if it is not.
   */
  public boolean getBit(int bit)
  {
    return bitset.get(bit);
  }

  /**
   * Set a single bit in the Bloom filter.
   *
   * @param bit
   *          is the bit to set.
   * @param value
   *          If true, the bit is set. If false, the bit is cleared.
   */
  public void setBit(int bit, boolean value)
  {
    bitset.set(bit, value);
  }

  /**
   * Return the bit set used to store the Bloom filter.
   *
   * @return bit set representing the Bloom filter.
   */
  public BitSet getBitSet()
  {
    return bitset;
  }

  public void setBitSet(BitSet bitset)
  {
    this.bitset = bitset;
  }

  /**
   * Returns the number of bits in the Bloom filter. Use count() to retrieve the
   * number of inserted elements.
   *
   * @return the size of the bitset used by the Bloom filter.
   */
  public int size()
  {
    return this.bitSetSize;
  }

  /**
   * Returns the number of elements added to the Bloom filter after it was
   * constructed or after clear() was called.
   *
   * @return number of elements added to the Bloom filter.
   */
  public int count()
  {
    return this.numberOfAddedElements;
  }

  /**
   * Returns the expected number of elements to be inserted into the filter.
   * This value is the same value as the one passed to the constructor.
   *
   * @return expected number of elements.
   */
  public int getExpectedNumberOfElements()
  {
    return expectedNumberOfFilterElements;
  }

  /**
   * Get actual number of bits per element based on the number of elements that
   * have currently been inserted and the length of the Bloom filter. See also
   * getExpectedBitsPerElement().
   *
   * @return number of bits per element.
   */
  public double getBitsPerElement()
  {
    return this.bitSetSize / (double)numberOfAddedElements;
  }

  /**
   * Set the hasher in the Bloom filter.
   *
   * @param hasher
   *          is the hash function to set.
   */

  public void setHasher(HashFunction hasher)
  {
    this.hasher = hasher;
  }

  public static final class HashFunction
  {
    private static final long SEED = 0x7f3a21eaL;
    private static long X64_128_C1 = 0x87c37b91114253d5L;
    private static long X64_128_C2 = 0x4cf5ad432745937fL;
    /**
     * Helps convert a byte into its unsigned value
     */
    public static final int UNSIGNED_MASK = 0xff;

    /**
     * Return the hash of the bytes as long.
     *
     * @param bytes
     *          the bytes to be hashed
     *
     * @return the generated hash value
     */
    public long hash(Slice slice)
    {
      long h1 = SEED;
      long h2 = SEED;

      //the offset related to the begin of slice
      int relativeOffset = 0;
      while (slice.length - relativeOffset >= 16) {
        long k1 = getLong(slice, slice.offset + relativeOffset);
        relativeOffset += 8; //size of long count by int
        long k2 = getLong(slice, slice.offset + relativeOffset);
        relativeOffset += 8;

        h1 ^= mixK1(k1);

        h1 = Long.rotateLeft(h1, 27);
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729;

        h2 ^= mixK2(k2);

        h2 = Long.rotateLeft(h2, 31);
        h2 += h1;
        h2 = h2 * 5 + 0x38495ab5;
      }

      if (slice.length > relativeOffset) {
        long k1 = 0;
        long k2 = 0;
        int absoluteOffset = slice.offset + relativeOffset;
        switch (slice.length - relativeOffset) {
          case 15:
            k2 ^= (long)(slice.buffer[absoluteOffset + 14] & UNSIGNED_MASK) << 48; // fall through

          case 14:
            k2 ^= (long)(slice.buffer[absoluteOffset + 13] & UNSIGNED_MASK) << 40; // fall through

          case 13:
            k2 ^= (long)(slice.buffer[absoluteOffset + 12] & UNSIGNED_MASK) << 32; // fall through

          case 12:
            k2 ^= (long)(slice.buffer[absoluteOffset + 11] & UNSIGNED_MASK) << 24; // fall through

          case 11:
            k2 ^= (long)(slice.buffer[absoluteOffset + 10] & UNSIGNED_MASK) << 16; // fall through

          case 10:
            k2 ^= (long)(slice.buffer[absoluteOffset + 9] & UNSIGNED_MASK) << 8; // fall through

          case 9:
            k2 ^= slice.buffer[absoluteOffset + 8] & UNSIGNED_MASK; // fall through

          case 8:
            k1 ^= getLong(slice, absoluteOffset);
            break;

          case 7:
            k1 ^= (long)(slice.buffer[absoluteOffset + 6] & UNSIGNED_MASK) << 48; // fall through

          case 6:
            k1 ^= (long)(slice.buffer[absoluteOffset + 5] & UNSIGNED_MASK) << 40; // fall through

          case 5:
            k1 ^= (long)(slice.buffer[absoluteOffset + 4] & UNSIGNED_MASK) << 32; // fall through

          case 4:
            k1 ^= (long)(slice.buffer[absoluteOffset + 3] & UNSIGNED_MASK) << 24; // fall through

          case 3:
            k1 ^= (long)(slice.buffer[absoluteOffset + 2] & UNSIGNED_MASK) << 16; // fall through

          case 2:
            k1 ^= (long)(slice.buffer[absoluteOffset + 1] & UNSIGNED_MASK) << 8; // fall through

          case 1:
            k1 ^= slice.buffer[absoluteOffset] & UNSIGNED_MASK;
            break;

          default:
            throw new AssertionError("Code should not reach here!");
        }

        // mix
        h1 ^= mixK1(k1);
        h2 ^= mixK2(k2);
      }

      // ----------
      // finalization

      h1 ^= slice.length;
      h2 ^= slice.length;

      h1 += h2;
      h2 += h1;

      h1 = fmix64(h1);
      h2 = fmix64(h2);

      h1 += h2;
      h2 += h1;

      return h1;
    }

    private static long getLong(Slice slice, int absoluteOffset)
    {
      return ((((long)slice.buffer[absoluteOffset++]) << 56) |
          (((long)slice.buffer[absoluteOffset++] & 0xff) << 48) |
          (((long)slice.buffer[absoluteOffset++] & 0xff) << 40) |
          (((long)slice.buffer[absoluteOffset++] & 0xff) << 32) |
          (((long)slice.buffer[absoluteOffset++] & 0xff) << 24) |
          (((long)slice.buffer[absoluteOffset++] & 0xff) << 16) |
          (((long)slice.buffer[absoluteOffset++] & 0xff) <<  8) |
          (((long)slice.buffer[absoluteOffset++] & 0xff)      ));
    }

    private static long mixK1(long k1)
    {
      k1 *= X64_128_C1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= X64_128_C2;

      return k1;
    }

    private static long mixK2(long k2)
    {
      k2 *= X64_128_C2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= X64_128_C1;

      return k2;
    }

    private static long fmix64(long k)
    {
      k ^= k >>> 33;
      k *= 0xff51afd7ed558ccdL;
      k ^= k >>> 33;
      k *= 0xc4ceb9fe1a85ec53L;
      k ^= k >>> 33;

      return k;
    }
  }
}
