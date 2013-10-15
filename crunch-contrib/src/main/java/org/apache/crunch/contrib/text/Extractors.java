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
package org.apache.crunch.contrib.text;

import java.lang.reflect.Constructor;
import java.util.Collection;

import org.apache.crunch.Pair;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Factory methods for constructing common {@code Extractor} types.
 */
public final class Extractors {
  
  /**
   * Returns an Extractor for integers.
   */
  public static Extractor<Integer> xint() {
    return xint(0);
  }

  /**
   * Returns an Extractor for integers.
   */
  public static Extractor<Integer> xint(Integer defaultValue) {
    return new IntExtractor(defaultValue);
  }

  /**
   * Returns an Extractor for longs.
   */
  public static Extractor<Long> xlong() {
    return xlong(0L);
  }
  
  /**
   * Returns an Extractor for longs.
   */
  public static Extractor<Long> xlong(Long defaultValue) {
    return new LongExtractor(defaultValue);
  }
  
  /**
   * Returns an Extractor for floats.
   */
  public static Extractor<Float> xfloat() {
    return xfloat(0.0f);
  }
  
  public static Extractor<Float> xfloat(Float defaultValue) {
    return new FloatExtractor(defaultValue);
  }
  
  /**
   * Returns an Extractor for doubles.
   */
  public static Extractor<Double> xdouble() {
    return xdouble(0.0);
  }

  public static Extractor<Double> xdouble(Double defaultValue) {
    return new DoubleExtractor(defaultValue);
  }
  
  /**
   * Returns an Extractor for booleans.
   */
  public static Extractor<Boolean> xboolean() {
    return xboolean(false);
  }

  public static Extractor<Boolean> xboolean(Boolean defaultValue) {
    return new BooleanExtractor(defaultValue);
  }

  /**
   * Returns an Extractor for strings.
   */
  public static Extractor<String> xstring() {
    return xstring("");
  }

  public static Extractor<String> xstring(String defaultValue) {
    return new StringExtractor(defaultValue);
  }

  public static <T> Extractor<Collection<T>> xcollect(TokenizerFactory scannerFactory, Extractor<T> extractor) {
    return new CollectionExtractor<T>(scannerFactory, extractor);
  }
  
  /**
   * Returns an Extractor for pairs of the given types that uses the given {@code TokenizerFactory}
   * for parsing the sub-fields.
   */
  public static <K, V> Extractor<Pair<K, V>> xpair(TokenizerFactory scannerFactory,
      Extractor<K> one, Extractor<V> two) {
    return new PairExtractor<K, V>(scannerFactory, one, two);
  }
  
  /**
   * Returns an Extractor for triples of the given types that uses the given {@code TokenizerFactory}
   * for parsing the sub-fields.
   */
  public static <A, B, C> Extractor<Tuple3<A, B, C>> xtriple(TokenizerFactory scannerFactory, Extractor<A> a,
      Extractor<B> b, Extractor<C> c) {
    return new TripExtractor<A, B, C>(scannerFactory, a, b, c);
  }
  
  /**
   * Returns an Extractor for quads of the given types that uses the given {@code TokenizerFactory}
   * for parsing the sub-fields.
   */
  public static <A, B, C, D> Extractor<Tuple4<A, B, C, D>> xquad(TokenizerFactory scannerFactory, Extractor<A> a,
      Extractor<B> b, Extractor<C> c, Extractor<D> d) {
    return new QuadExtractor<A, B, C, D>(scannerFactory, a, b, c, d);
  }
  
  /**
   * Returns an Extractor for an arbitrary number of types that uses the given {@code TokenizerFactory}
   * for parsing the sub-fields.
   */
  public static Extractor<TupleN> xtupleN(TokenizerFactory scannerFactory, Extractor...extractors) {
    return new TupleNExtractor(scannerFactory, extractors);
  }
  
  /**
   * Returns an Extractor for a subclass of {@code Tuple} with a constructor that
   * has the given extractor types that uses the given {@code TokenizerFactory}
   * for parsing the sub-fields.
   */
  public static <T extends Tuple> Extractor<T> xcustom(Class<T> clazz, TokenizerFactory scannerFactory, Extractor... extractors) {
    return new CustomTupleExtractor<T>(scannerFactory, clazz, extractors);
  }
  
  private static class IntExtractor extends AbstractSimpleExtractor<Integer> {
    
    IntExtractor(Integer defaultValue) {
      super(defaultValue);
    }
    
    @Override
    protected Integer doExtract(Tokenizer tokenizer) {
      return tokenizer.nextInt();
    }

    @Override
    public PType<Integer> getPType(PTypeFamily ptf) {
      return ptf.ints();
    }
    
    @Override
    public String toString() {
      return "xint";
    }
  }
  
  private static class LongExtractor extends AbstractSimpleExtractor<Long> {
    LongExtractor(Long defaultValue) {
      super(defaultValue);
    }
    
    @Override
    protected Long doExtract(Tokenizer tokenizer) {
      return tokenizer.nextLong();
    }

    @Override
    public PType<Long> getPType(PTypeFamily ptf) {
      return ptf.longs();
    }
    
    @Override
    public String toString() {
      return "xlong";
    }
  }

  private static class FloatExtractor extends AbstractSimpleExtractor<Float> {
    FloatExtractor(Float defaultValue) {
      super(defaultValue);
    }
    
    @Override
    protected Float doExtract(Tokenizer tokenizer) {
      return tokenizer.nextFloat();
    }

    @Override
    public PType<Float> getPType(PTypeFamily ptf) {
      return ptf.floats();
    }
    
    @Override
    public String toString() {
      return "xfloat";
    }
  }

  private static class DoubleExtractor extends AbstractSimpleExtractor<Double> {
    DoubleExtractor(Double defaultValue) {
      super(defaultValue);
    }
    
    @Override
    protected Double doExtract(Tokenizer tokenizer) {
      return tokenizer.nextDouble();
    }

    @Override
    public PType<Double> getPType(PTypeFamily ptf) {
      return ptf.doubles();
    }
    
    @Override
    public String toString() {
      return "xdouble";
    }
  }

  private static class BooleanExtractor extends AbstractSimpleExtractor<Boolean> {
    
    BooleanExtractor(Boolean defaultValue) {
      super(defaultValue);
    }
    
    @Override
    protected Boolean doExtract(Tokenizer tokenizer) {
      return tokenizer.nextBoolean();
    }

    @Override
    public PType<Boolean> getPType(PTypeFamily ptf) {
      return ptf.booleans();
    }
    
    @Override
    public String toString() {
      return "xboolean";
    }
  }

  private static class StringExtractor extends AbstractSimpleExtractor<String> {
    
    StringExtractor(String defaultValue) {
      super(defaultValue);
    }
    
    @Override
    protected String doExtract(Tokenizer tokenizer) {
      return tokenizer.next();
    }

    @Override
    public PType<String> getPType(PTypeFamily ptf) {
      return ptf.strings();
    }
    
    @Override
    public String toString() {
      return "xstring";
    }
  }

  private static class CollectionExtractor<T> implements Extractor<Collection<T>> {

    private final TokenizerFactory tokenizerFactory;
    private final Extractor<T> extractor;
    private int errors = 0;
    private boolean errorOnLast;

    CollectionExtractor(TokenizerFactory tokenizerFactory, Extractor<T> extractor) {
      this.tokenizerFactory = tokenizerFactory;
      this.extractor = extractor;
    }
    
    @Override
    public Collection<T> extract(String input) {
      errorOnLast = false;
      Tokenizer tokenizer = tokenizerFactory.create(input);
      Collection<T> parsed = Lists.newArrayList();
      while (tokenizer.hasNext()) {
        parsed.add(extractor.extract(tokenizer.next()));
        if (extractor.errorOnLastRecord() && !errorOnLast) {
          errorOnLast = true;
          errors++;
        }
      }
      return parsed;
    }

    @Override
    public PType<Collection<T>> getPType(PTypeFamily ptf) {
      return ptf.collections(extractor.getPType(ptf));
    }

    @Override
    public Collection<T> getDefaultValue() {
      return ImmutableList.of();
    }

    @Override
    public ExtractorStats getStats() {
      return new ExtractorStats(errors,
          ImmutableList.of(extractor.getStats().getErrorCount()));
    }

    @Override
    public void initialize() {
      this.errorOnLast = false;
      this.errors = 0;
      extractor.initialize();
    }
    
    @Override
    public boolean errorOnLastRecord() {
      return errorOnLast;
    }
    
  }
  
  private static class PairExtractor<K, V> extends AbstractCompositeExtractor<Pair<K, V>> {
    private final Extractor<K> one;
    private final Extractor<V> two;
    
    PairExtractor(TokenizerFactory scannerFactory, Extractor<K> one, Extractor<V> two) {
      super(scannerFactory, ImmutableList.<Extractor<?>>of(one, two));
      this.one = one;
      this.two = two;
    }

    @Override
    protected Pair<K, V> doCreate(Object[] values) {
      return Pair.of((K) values[0], (V) values[1]);
    }
    
    @Override
    public PType<Pair<K, V>> getPType(PTypeFamily ptf) {
      return ptf.pairs(one.getPType(ptf), two.getPType(ptf));
    }
    
    @Override
    public String toString() {
      return "xpair(" + one + "," + two + ")";
    }

    @Override
    public Pair<K, V> getDefaultValue() {
      return Pair.of(one.getDefaultValue(), two.getDefaultValue());
    }
  }

  private static class TripExtractor<A, B, C> extends AbstractCompositeExtractor<Tuple3<A, B, C>> {
    private final Extractor<A> one;
    private final Extractor<B> two;
    private final Extractor<C> three;
    
    TripExtractor(TokenizerFactory sf, Extractor<A> one, Extractor<B> two, Extractor<C> three) {
      super(sf, ImmutableList.<Extractor<?>>of(one, two, three));
      this.one = one;
      this.two = two;
      this.three = three;
    }

    @Override
    protected Tuple3<A, B, C> doCreate(Object[] values) {
      return Tuple3.of((A) values[0], (B) values[1], (C) values[2]);
    }
    
    @Override
    public PType<Tuple3<A, B, C>> getPType(PTypeFamily ptf) {
      return ptf.triples(one.getPType(ptf), two.getPType(ptf), three.getPType(ptf));
    }
    
    @Override
    public Tuple3<A, B, C> getDefaultValue() {
      return Tuple3.of(one.getDefaultValue(), two.getDefaultValue(), three.getDefaultValue());
    }
    
    @Override
    public String toString() {
      return "xtriple(" + one + "," + two + "," + three + ")";
    }
  }

  private static class QuadExtractor<A, B, C, D> extends AbstractCompositeExtractor<Tuple4<A, B, C, D>> {
    private final Extractor<A> one;
    private final Extractor<B> two;
    private final Extractor<C> three;
    private final Extractor<D> four;
    
    QuadExtractor(TokenizerFactory sf, Extractor<A> one, Extractor<B> two, Extractor<C> three,
        Extractor<D> four) {
      super(sf, ImmutableList.<Extractor<?>>of(one, two, three, four));
      this.one = one;
      this.two = two;
      this.three = three;
      this.four = four;
    }
    
    @Override
    protected Tuple4<A, B, C, D> doCreate(Object[] values) {
      return Tuple4.of((A) values[0], (B) values[1], (C) values[2], (D) values[3]);
    }

    @Override
    public PType<Tuple4<A, B, C, D>> getPType(PTypeFamily ptf) {
      return ptf.quads(one.getPType(ptf), two.getPType(ptf), three.getPType(ptf),
          four.getPType(ptf));
    }
    
    @Override
    public Tuple4<A, B, C, D> getDefaultValue() {
      return Tuple4.of(one.getDefaultValue(), two.getDefaultValue(), three.getDefaultValue(),
          four.getDefaultValue());
    }
    
    @Override
    public String toString() {
      return "xquad(" + one + "," + two + "," + three + "," + four + ")";
    }
  }

  private static class TupleNExtractor extends AbstractCompositeExtractor<TupleN> {
    private final Extractor[] extractors;
    
    TupleNExtractor(TokenizerFactory scannerFactory, Extractor...extractors) {
      super(scannerFactory, ImmutableList.<Extractor<?>>copyOf(extractors));
      this.extractors = extractors;
    }
    
    @Override
    protected TupleN doCreate(Object[] values) {
      return new TupleN(values);
    }

    @Override
    public PType<TupleN> getPType(PTypeFamily ptf) {
      PType[] ptypes = new PType[extractors.length];
      for (int i = 0; i < ptypes.length; i++) {
        ptypes[i] = extractors[i].getPType(ptf);
      }
      return ptf.tuples(ptypes);
    }
    
    @Override
    public TupleN getDefaultValue() {
      Object[] values = new Object[extractors.length];
      for (int i = 0; i < values.length; i++) {
        values[i] = extractors[i].getDefaultValue();
      }
      return doCreate(values);
    }
    
    @Override
    public String toString() {
      return "xtupleN(" + Joiner.on(',').join(extractors) + ")";
    }
  }

  private static class CustomTupleExtractor<T extends Tuple> extends AbstractCompositeExtractor<T> {

    private final Class<T> clazz;
    private final Extractor[] extractors;
    
    private transient Constructor<T> constructor;
    
    CustomTupleExtractor(TokenizerFactory sf, Class<T> clazz, Extractor... extractors) {
      super(sf, ImmutableList.<Extractor<?>>copyOf(extractors));
      this.clazz = clazz;
      this.extractors = extractors;
    }
    
    @Override
    public void initialize() {
      super.initialize();
      
      Class[] typeArgs = new Class[extractors.length];
      for (int i = 0; i < typeArgs.length; i++) {
        typeArgs[i] = extractors[i].getPType(
            AvroTypeFamily.getInstance()).getTypeClass();
      }
      try {
        constructor = clazz.getConstructor(typeArgs);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    public T doCreate(Object[] values) {
      try {
        return constructor.newInstance(values);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PType<T> getPType(PTypeFamily ptf) {
      PType[] ptypes = new PType[extractors.length];
      for (int i = 0; i < ptypes.length; i++) {
        ptypes[i] = extractors[i].getPType(ptf);
      }
      return ptf.tuples(clazz, ptypes);
    }

    @Override
    public T getDefaultValue() {
      Object[] values = new Object[extractors.length];
      for (int i = 0; i < values.length; i++) {
        values[i] = extractors[i].getDefaultValue();
      }
      return doCreate(values);
    }

    @Override
    public String toString() {
      return "Extractor(" + clazz + ")";
    }
  }
}
