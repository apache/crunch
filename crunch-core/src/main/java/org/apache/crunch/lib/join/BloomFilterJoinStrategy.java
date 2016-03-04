/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.lib.join;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.ReadableData;
import org.apache.crunch.fn.ExtractKeyFn;
import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableType;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * Join strategy that uses a <a href="http://en.wikipedia.org/wiki/Bloom_filter">Bloom filter</a>
 * that is trained on the keys of the left-side table to filter the key/value pairs of the right-side
 * table before sending through the shuffle and reduce phase.
 * <p>
 * This strategy is useful in cases where the right-side table contains many keys that are not
 * present in the left-side table. In this case, the use of the Bloom filter avoids a
 * potentially costly shuffle phase for data that would never be joined to the left side.
 * <p>
 * Implementation Note: right and full outer join type are handled by splitting the right-side
 * table (the bigger one) into two disjunctive streams: negatively filtered (right outer part)
 * and positively filtered (passed to delegate strategy).
 */
public class BloomFilterJoinStrategy<K, U, V> implements JoinStrategy<K, U, V> {

  private int vectorSize;
  private int nbHash;
  private JoinStrategy<K, U, V> delegateJoinStrategy;

  /**
   * Instantiate with the expected number of unique keys in the left table.
   * <p>
   * The {@link DefaultJoinStrategy} will be used to perform the actual join after filtering.
   * 
   * @param numElements expected number of unique keys
   */
  public BloomFilterJoinStrategy(int numElements) {
    this(numElements, 0.05f);
  }
  
  /**
   * Instantiate with the expected number of unique keys in the left table, and the acceptable
   * false positive rate for the Bloom filter.
   * <p>
   * The {@link DefaultJoinStrategy} will be used to perform the actual join after filtering.
   * 
   * @param numElements expected number of unique keys
   * @param falsePositiveRate acceptable false positive rate for Bloom Filter
   */
  public BloomFilterJoinStrategy(int numElements, float falsePositiveRate) {
    this(numElements, falsePositiveRate, new DefaultJoinStrategy<K, U, V>());
  }
  
  /**
   * Instantiate with the expected number of unique keys in the left table, and the acceptable
   * false positive rate for the Bloom filter, and an underlying join strategy to delegate to.
   * 
   * @param numElements expected number of unique keys
   * @param falsePositiveRate acceptable false positive rate for Bloom Filter
   * @param delegateJoinStrategy join strategy to delegate to after filtering
   */
  public BloomFilterJoinStrategy(int numElements, float falsePositiveRate, JoinStrategy<K,U,V> delegateJoinStrategy) {
    this.vectorSize = getOptimalVectorSize(numElements, falsePositiveRate);
    this.nbHash = getOptimalNumHash(numElements, vectorSize);
    this.delegateJoinStrategy = delegateJoinStrategy;
  }
  
  /**
   * Calculates the optimal vector size for a given number of elements and acceptable false
   * positive rate.
   */
  private static int getOptimalVectorSize(int numElements, float falsePositiveRate) {
    return (int) (-numElements * (float)Math.log(falsePositiveRate) / Math.pow(Math.log(2), 2));
  }
  
  /**
   * Calculates the optimal number of hash functions to be used.
   */
  private static int getOptimalNumHash(int numElements, float vectorSize) {
    return (int)Math.round(vectorSize * Math.log(2) / numElements);
  }
  
  @Override
  public PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right, JoinType joinType) {

    PType<BloomFilter> bloomFilterType = getBloomFilterType(left.getTypeFamily());
    PCollection<BloomFilter> bloomFilters = left.keys().parallelDo(
        "Create bloom filters",
        new CreateBloomFilterFn<>(vectorSize, nbHash, left.getKeyType()),
        bloomFilterType);

    ReadableData<BloomFilter> bloomData = bloomFilters.asReadable(true);
    FilterKeysWithBloomFilterFn<K, V> filterKeysFn = new FilterKeysWithBloomFilterFn<>(
        bloomData, vectorSize, nbHash, left.getKeyType());

    if (joinType != JoinType.INNER_JOIN && joinType != JoinType.LEFT_OUTER_JOIN) {
      right = right.parallelDo(
          "disable deep copy", new DeepCopyDisablerFn<Pair<K, V>>(), right.getPTableType());
    }

    ParallelDoOptions options = ParallelDoOptions.builder()
        .sourceTargets(bloomData.getSourceTargets()).build();
    PTable<K, V> filteredRightSide = right.parallelDo(
        "Filter right-side with BloomFilters",
        filterKeysFn, right.getPTableType(), options);

    PTable<K, Pair<U, V>> leftJoinedWithFilteredRight = delegateJoinStrategy
        .join(left, filteredRightSide, joinType);

    if (joinType == JoinType.INNER_JOIN || joinType == JoinType.LEFT_OUTER_JOIN) {
      return leftJoinedWithFilteredRight;
    }

    return leftJoinedWithFilteredRight.union(
        right
            .parallelDo(
                "Negatively filter right-side with BloomFilters",
                FilterFns.not(filterKeysFn), right.getPTableType(), options)
            .mapValues(
                "Right outer join: attach null as left-value",
                new NullKeyFn<U, V>(), leftJoinedWithFilteredRight.getValueType()));
  }
  
  /**
   * Creates Bloom filter(s) for filtering of right-side keys. 
   */
  private static class CreateBloomFilterFn<K> extends DoFn<K, BloomFilter> {

    private int vectorSize;
    private int nbHash;
    private transient BloomFilter bloomFilter;
    private transient MapFn<K,byte[]> keyToBytesFn;
    private PType<K> ptype;
    
    CreateBloomFilterFn(int vectorSize, int nbHash, PType<K> ptype) {
      this.vectorSize = vectorSize;
      this.nbHash = nbHash;
      this.ptype = ptype;
    }
    
    @Override
    public void initialize() {
      super.initialize();
      bloomFilter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
      ptype.initialize(getConfiguration());
      keyToBytesFn = getKeyToBytesMapFn(ptype, getConfiguration());
    }
    
    @Override
    public void process(K input, Emitter<BloomFilter> emitter) {
      bloomFilter.add(new Key(keyToBytesFn.map(input)));
    }
    
    @Override
    public void cleanup(Emitter<BloomFilter> emitter) {
      emitter.emit(bloomFilter);
    }
    
  }
  
  /**
   * Filters right-side keys with a Bloom filter before passing them off to the delegate join strategy.
   */
  private static class FilterKeysWithBloomFilterFn<K,V> extends FilterFn<Pair<K, V>> {
    
    private int vectorSize;
    private int nbHash;
    private PType<K> keyType;
    private PType<BloomFilter> bloomFilterPType;
    private transient BloomFilter bloomFilter;
    private transient MapFn<K,byte[]> keyToBytesFn;
    private ReadableData<BloomFilter> bloomData;

    FilterKeysWithBloomFilterFn(ReadableData<BloomFilter> bloomData, int vectorSize, int nbHash, PType<K> keyType) {
      this.bloomData = bloomData;
      this.vectorSize = vectorSize;
      this.nbHash = nbHash;
      this.keyType = keyType;
    }
    
    
    @Override
    public void configure(Configuration conf) {
      bloomData.configure(conf);
    }
    
    @Override
    public void initialize() {
      super.initialize();
      
      keyType.initialize(getConfiguration());
      
      keyToBytesFn = getKeyToBytesMapFn(keyType, getConfiguration());

      Iterable<BloomFilter> iterable;
      try {
        iterable = bloomData.read(getContext());
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error reading right-side of map side join: ", e);
      }

      bloomFilter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
      for (BloomFilter subFilter : iterable) {
        bloomFilter.or(subFilter);
      }
    }

    @Override
    public boolean accept(Pair<K, V> input) {
      Key key = new Key(keyToBytesFn.map(input.first()));
      return bloomFilter.membershipTest(key);
    }
  }
  
  /**
   * Returns the appropriate MapFn for converting the key type into byte arrays.
   */
  private static <K> MapFn<K,byte[]> getKeyToBytesMapFn(PType<K> ptype, Configuration conf) {
    if (ptype instanceof AvroType) {
      return new AvroToBytesFn<K>((AvroType)ptype, conf);
    }
    if (ptype instanceof WritableType) {
      return new WritableToBytesFn<K>((WritableType)ptype, conf);
    }
    throw new IllegalStateException("Unrecognized PType: " + ptype);
  }
  
  /**
   * Returns the appropriate PType for serializing BloomFilters using the same
   * type family as is used for the input collections.
   */
  private static PType<BloomFilter> getBloomFilterType(PTypeFamily typeFamily) {
    if (typeFamily.equals(AvroTypeFamily.getInstance())) {
      return Avros.writables(BloomFilter.class);
    } else if (typeFamily.equals(WritableTypeFamily.getInstance())) {
      return Writables.writables(BloomFilter.class);
    } else {
      throw new IllegalStateException("Unrecognized PTypeFamily: " + typeFamily);
    }
  }
  
  /**
   * Converts a Writable into a byte array so that it can be added to a BloomFilter.
   */
  private static class WritableToBytesFn<T> extends MapFn<T,byte[]>{
    
    private WritableType<T,?> ptype;
    private DataOutputBuffer dataOutputBuffer;
    
    WritableToBytesFn(WritableType<T,?> ptype, Configuration conf) {
      this.ptype = ptype;
      dataOutputBuffer = new DataOutputBuffer();
    }

    @Override
    public byte[] map(T input) {
      dataOutputBuffer.reset();
      Writable writable = (Writable) ptype.getOutputMapFn().map(input);
      try {
        writable.write(dataOutputBuffer);
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }
      byte[] output = new byte[dataOutputBuffer.getLength()];
      System.arraycopy(dataOutputBuffer.getData(), 0, output, 0, dataOutputBuffer.getLength());
      return output;
    }
    
  }
  
  /**
   * Converts an Avro value into a byte array so that it can be added to a Bloom filter.
   */
  private static class AvroToBytesFn<T> extends MapFn<T,byte[]> {
    
    private AvroType<T> ptype;
    private BinaryEncoder encoder;
    private DatumWriter datumWriter;
    
    AvroToBytesFn(AvroType<T> ptype, Configuration conf) {
      this.ptype = ptype;
      datumWriter = AvroMode.fromType(ptype).withFactoryFromConfiguration(conf)
          .getWriter(ptype.getSchema());
    }

    @Override
    public byte[] map(T input) {
      Object datum = ptype.getOutputMapFn().map(input);
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, encoder);
      try {
        datumWriter.write(datum, encoder);
        encoder.flush();
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }
      return byteArrayOutputStream.toByteArray();
    }
    
  }

  /**
   * Converts value into a null-value pair. It is used to convert negatively filtered
   * right-side values into right outer join part.
   */
  private static class NullKeyFn<K, V> extends ExtractKeyFn<K, V> {
    public NullKeyFn() {
      super(new MapFn<V, K>() {
        @Override public K map(V input) {
          return null;
        }

        @Override public float scaleFactor() {
          return 0.0001f;
        }
      });
    }
  }

  /**
   * Right and full outer join types are handled by splitting the right-side table (the bigger one)
   * into two disjunctive streams: negatively filtered (right outer part) and positively filtered.
   * To prevent concurrent modification Crunch performs a deep copy of such a splitted stream by
   * default (see {@link DoFn#disableDeepCopy()}), which introduces an extra overhead. Since Bloom
   * Filter directs every record to exactly one of these streams, making concurrent modification
   * impossible, we can safely disable this feature. To achieve this we put the {@code right} PTable
   * through a {@code parallelDo} call with this {@code DoFn}.
   */
  private static class DeepCopyDisablerFn<T> extends MapFn<T, T> {

    @Override
    public T map(T input) {
      return input;
    }

    @Override
    public boolean disableDeepCopy() {
      return true;
    }
  }
}
