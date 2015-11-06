/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket.bloomFilter;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Collection;

import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.sangupta.murmur.Murmur3;

/**
 * A Bloom Filter implementation
 *
 * @param <T>
 */
public class BloomFilter<T> implements Serializable
{
  private static final long serialVersionUID = 1113436340128152699L;
  @Bind(JavaSerializer.class)
  private BitSet bitset;
  private int bitSetSize;
  private double bitsPerElement;
  private int expectedNumberOfFilterElements; // expected (maximum) number of elements to be added
  private int numberOfAddedElements; // number of elements actually added to the Bloom filter
  private int numberOfHashes; // number of hash functions
  protected transient HashFunction hasher = new HashFunction();
  protected transient Decomposer<T> customDecomposer = new Decomposer.DefaultDecomposer<T>();

  /**
   * Set the attributes to the empty Bloom filter. The total length of the Bloom filter will be c*n.
   *
   * @param c
   *          is the number of bits used per element.
   * @param n
   *          is the expected number of elements the filter will contain.
   * @param k
   *          is the number of hash functions used.
   */
  private void SetAttributes(double c, int n, int k)
  {
    this.expectedNumberOfFilterElements = n;
    this.numberOfHashes = k;
    this.bitsPerElement = c;
    this.bitSetSize = (int)Math.ceil(c * n);
    numberOfAddedElements = 0;
    this.bitset = new BitSet(bitSetSize);
  }

  /**
   * Constructs an empty Bloom filter with a given false positive probability.
   *
   * @param expectedNumberOfElements
   *          is the expected number of elements the filter will contain.
   * @param falsePositiveProbability
   *          is the desired false positive probability.
   */
  public BloomFilter(int expectedNumberOfElements, double falsePositiveProbability)
  {
    if (this.bitset == null) {
      SetAttributes(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2), // c = k / ln(2)
          expectedNumberOfElements, (int)Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))));
    }
  }

  /**
   * Constructs an empty Bloom filter with a given decomposer and false positive probability..
   *
   * @param expectedNumberOfElements
   *          is the expected number of elements the filter will contain.
   * @param falsePositiveProbability
   *          is the desired false positive probability.
   * @param decomposer
   *          is the custom decomposer used to decompose the object to bytes.
   */
  public BloomFilter(int expectedNumberOfElements, double falsePositiveProbability, Decomposer<T> decomposer,
      HashFunction hasher)
  {
    this(expectedNumberOfElements, falsePositiveProbability);
    if (decomposer != null) {
      this.customDecomposer = decomposer;
    }
    if (hasher != null) {
      this.hasher = hasher;
    }
  }

  /**
   * Generate integer array based on the hash function till the number of hashes.
   *
   * @param data
   *          specifies input data.
   * @return array of int-sized hashes
   */
  private int[] createHashes(byte[] data)
  {
    int[] result = new int[numberOfHashes];
    long hash64 = hasher.hash(data);
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
   * Calculates the expected probability of false positives based on the number of expected filter elements and the size
   * of the Bloom filter.
   *
   * @return expected probability of false positives.
   */
  public double expectedFalsePositiveProbability()
  {
    return getFalsePositiveProbability(expectedNumberOfFilterElements);
  }

  /**
   * Calculate the probability of a false positive given the specified number of inserted elements.
   *
   * @param numberOfElements
   *          number of inserted elements.
   * @return probability of a false positive.
   */
  public double getFalsePositiveProbability(double numberOfElements)
  {
    // (1 - e^(-k * n / m)) ^ k
    return Math.pow((1 - Math.exp(-numberOfHashes * (double)numberOfElements / (double)bitSetSize)), numberOfHashes);
  }

  /**
   * Get the current probability of a false positive. The probability is calculated from the size of the Bloom filter
   * and the current number of elements added to it.
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
   * numberOfHashes is the optimal number of hash functions based on the size of the Bloom filter and the expected
   * number of inserted elements.
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
   * Adds an object to the Bloom filter. The output from the object's toString() method is used as input to the hash
   * functions.
   *
   * @param tuple
   *          is an element to register in the Bloom filter.
   */
  public void add(T tuple)
  {
    add(customDecomposer.decompose(tuple));
  }

  /**
   * Adds an array of bytes to the Bloom filter.
   *
   * @param bytes
   *          array of bytes to add to the Bloom filter.
   */
  private void add(byte[] bytes)
  {
    int[] hashes = createHashes(bytes);
    for (int hash : hashes) {
      bitset.set(Math.abs(hash % bitSetSize), true);
    }
    numberOfAddedElements++;
  }

  /**
   * Returns true if the element could have been inserted into the Bloom filter. Use getFalsePositiveProbability() to
   * calculate the probability of this being correct.
   *
   * @param element
   *          element to check.
   * @return true if the element could have been inserted into the Bloom filter.
   */
  public boolean contains(T element)
  {
    return contains(customDecomposer.decompose(element));
  }

  /**
   * Returns true if the array of bytes could have been inserted into the Bloom filter. Use
   * getFalsePositiveProbability() to calculate the probability of this being correct.
   *
   * @param bytes
   *          array of bytes to check.
   * @return true if the array could have been inserted into the Bloom filter.
   */
  private boolean contains(byte[] bytes)
  {
    int[] hashes = createHashes(bytes);
    for (int hash : hashes) {
      if (!bitset.get(Math.abs(hash % bitSetSize))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if all the elements of a Collection could have been inserted into the Bloom filter. Use
   * getFalsePositiveProbability() to calculate the probability of this being correct.
   *
   * @param c
   *          elements to check.
   * @return true if all the elements in c could have been inserted into the Bloom filter.
   */
  public boolean containsAll(Collection<? extends T> c)
  {
    for (T element : c) {
      if (!contains(element)) {
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
   * Returns the number of bits in the Bloom filter. Use count() to retrieve the number of inserted elements.
   *
   * @return the size of the bitset used by the Bloom filter.
   */
  public int size()
  {
    return this.bitSetSize;
  }

  /**
   * Returns the number of elements added to the Bloom filter after it was constructed or after clear() was called.
   *
   * @return number of elements added to the Bloom filter.
   */
  public int count()
  {
    return this.numberOfAddedElements;
  }

  /**
   * Returns the expected number of elements to be inserted into the filter. This value is the same value as the one
   * passed to the constructor.
   *
   * @return expected number of elements.
   */
  public int getExpectedNumberOfElements()
  {
    return expectedNumberOfFilterElements;
  }

  /**
   * Get expected number of bits per element when the Bloom filter is full. This value is set by the constructor when
   * the Bloom filter is created. See also getBitsPerElement().
   *
   * @return expected number of bits per element.
   */
  public double getExpectedBitsPerElement()
  {
    return this.bitsPerElement;
  }

  /**
   * Get actual number of bits per element based on the number of elements that have currently been inserted and the
   * length of the Bloom filter. See also getExpectedBitsPerElement().
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

  /**
   * Set the customdecomposer in the Bloom filter.
   *
   * @param customDecomposer
   *          is the decomposer to set.
   */
  public void setCustomDecomposer(Decomposer<T> customDecomposer)
  {
    this.customDecomposer = customDecomposer;
  }

  class HashFunction
  {
    private static final long SEED = 0x7f3a21eaL;

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
}
