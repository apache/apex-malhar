package com.example.myapexapp;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class Codec3 extends KryoSerializableStreamCodec<Integer> {
    @Override
    public int getPartition(Integer tuple) {
      final int v = tuple;
      return (1 == (v & 1)) ? 0      // odd
           : (0 == (v & 3)) ? 1      // divisible by 4
           : 2;                      // divisible by 2 but not 4
    }
}
