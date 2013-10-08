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
package com.datatorrent.lib.algo;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.KeyValPair;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 *
 * Counts no. of unique values of a key.</br>
 * Partitions: yes, uses {@link UniqueCountUnifier} to merge partitioned output.
 * Stateful: no
 *
 * @since 0.3.5
 */
public class UniqueValueCount<K,V> extends BaseOperator {

    private Map<K,Set<V>>  interimUniqueValues= Maps.newHashMap();
    private transient boolean isPartitioned=false;


    @InputPortFieldAnnotation(name="inputPort")
    public transient DefaultInputPort<KeyValPair<K,V>> inputPort = new DefaultInputPort<KeyValPair<K, V>>() {
        @Override
        public void process(KeyValPair<K, V> pair) {
            Set<V> values= interimUniqueValues.get(pair.getKey());
            if(values==null){
                values=Sets.newHashSet();
                interimUniqueValues.put(pair.getKey(),values);
            }
            values.add(pair.getValue());
        }
    } ;

    @OutputPortFieldAnnotation(name="outputPort")
    public transient DefaultOutputPort<KeyValPair<K,Integer>> outputPort= new DefaultOutputPort<KeyValPair<K, Integer>>(){

        public Unifier<KeyValPair<K,Integer>> getUnifier() {
            return new UniqueCountUnifier<K,V>();
        }
    };


    @Override
    public void endWindow() {
        if(isPartitioned) {
            for(K key: interimUniqueValues.keySet()){
                outputPort.emit(new UniqueValueCountOutput<K,V>(key, interimUniqueValues.get(key)));
            }
        } else {
            for(K key: interimUniqueValues.keySet()){
                outputPort.emit(new KeyValPair<K, Integer>(key,interimUniqueValues.get(key).size()));
            }
        }
        interimUniqueValues.clear();
    }

    @Override
    public void setup(Context.OperatorContext operatorContext) {
        AttributeMap.Attribute<Integer> attr= operatorContext.getAttributes().attr(Context.OperatorContext.INITIAL_PARTITION_COUNT);
        isPartitioned= (attr!=null && attr.get()!=null && attr.get().intValue()>1 );
    }

    public static class UniqueValueCountOutput<K,V> extends KeyValPair<K,Integer> {

        private Set<V> interimUniqueValues=null;

        private UniqueValueCountOutput(){
            super(null,null);
        }
        /**
         * Constructor
         *
         * @param k sets key
         * @param v sets value
         */
        private UniqueValueCountOutput(K k, Integer v) {
            super(k, v);
        }

        private UniqueValueCountOutput(K k, Set<V> interimUniqueValues){
            super(k,null);
            this.interimUniqueValues=interimUniqueValues;
        }

        public Set<V> getInterimSet(){
            return interimUniqueValues;
        }

        @Override
        public String toString(){
            return super.toString();
        }
    }

    public static class UniqueCountUnifier<K,V> implements Unifier<KeyValPair<K,Integer>> {

        public final transient DefaultOutputPort<KeyValPair<K,Integer>> outputPort = new DefaultOutputPort<KeyValPair<K, Integer>>();

        private Map<K,Set<V>> finalUniqueValues= Maps.newHashMap();

        @Override
        public void process(KeyValPair<K,Integer> uniquePairFromPartitions) {
            if(uniquePairFromPartitions instanceof UniqueValueCountOutput) {
                UniqueValueCountOutput<K,V> pairList= (UniqueValueCountOutput<K,V>)uniquePairFromPartitions;
                Set<V> values= finalUniqueValues.get(pairList.getKey());
                if(values==null){
                    values=Sets.newHashSet();
                    finalUniqueValues.put(pairList.getKey(),values);
                }
                values.addAll(pairList.interimUniqueValues);
            }
        }

        @Override
        public void beginWindow(long l) {
        }

        @Override
        public void endWindow() {
            for(K key: finalUniqueValues.keySet()){
                outputPort.emit(new KeyValPair<K, Integer>(key,finalUniqueValues.get(key).size()));
            }
            finalUniqueValues.clear();
        }

        @Override
        public void setup(Context.OperatorContext operatorContext) {
        }

        @Override
        public void teardown() {
        }
    }
}
