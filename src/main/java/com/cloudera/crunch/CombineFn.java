/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.crunch;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A special {@link DoFn} implementation that converts an {@link Iterable}
 * of values into a single value. If a {@code CombineFn} instance is used
 * on a {@link PGroupedTable}, the function will be applied to the output
 * of the map stage before the data is passed to the reducer, which can
 * improve the runtime of certain classes of jobs.
 *
 */
public abstract class CombineFn<S, T> extends DoFn<Pair<S, Iterable<T>>, Pair<S, T>> {

  public static interface Aggregator<T> extends Serializable {
    Iterable<T> apply(Iterable<T> values);
  }
  
  public static class AggregatorCombineFn<K, V> extends CombineFn<K, V> {
    private final Aggregator<V> aggregator;
    
    public AggregatorCombineFn(Aggregator<V> aggregator) {
      this.aggregator = aggregator;
    }
    
    @Override
    public void process(Pair<K, Iterable<V>> input, Emitter<Pair<K, V>> emitter) {
      for (V v : aggregator.apply(input.second())) {
        emitter.emit(Pair.of(input.first(), v));
      }
    }    
  }
  
  @Override
  public float scaleFactor() {
    return 0.9f;
  }

  public static final <K, V> CombineFn<K, V> aggregator(Aggregator<V> aggregator) {
    return new AggregatorCombineFn<K, V>(aggregator);
  }
  
  public static final <K> CombineFn<K, Long> SUM_LONGS() {
    return aggregator(SUM_LONGS);
  }

  public static final <K> CombineFn<K, Integer> SUM_INTS() {
    return aggregator(SUM_INTS);
  }

  public static final <K> CombineFn<K, Float> SUM_FLOATS() {
    return aggregator(SUM_FLOATS);
  }

  public static final <K> CombineFn<K, Double> SUM_DOUBLES() {
    return aggregator(SUM_DOUBLES);
  }
  
  public static final <K> CombineFn<K, Long> MAX_LONGS() {
    return aggregator(MAX_LONGS);
  }

  public static final <K> CombineFn<K, Integer> MAX_INTS() {
    return aggregator(MAX_INTS);
  }

  public static final <K> CombineFn<K, Float> MAX_FLOATS() {
    return aggregator(MAX_FLOATS);
  }

  public static final <K> CombineFn<K, Double> MAX_DOUBLES() {
    return aggregator(MAX_DOUBLES);
  }
  
  public static final <K> CombineFn<K, Long> MIN_LONGS() {
    return aggregator(MIN_LONGS);
  }

  public static final <K> CombineFn<K, Integer> MIN_INTS() {
    return aggregator(MIN_INTS);
  }

  public static final <K> CombineFn<K, Float> MIN_FLOATS() {
    return aggregator(MIN_FLOATS);
  }

  public static final <K> CombineFn<K, Double> MIN_DOUBLES() {
    return aggregator(MIN_DOUBLES);
  }
  
  public static Aggregator<Long> SUM_LONGS = new Aggregator<Long>() {
    @Override
    public Iterable<Long> apply(Iterable<Long> next) {
      long sum = 0L;
      for (Long v : next) {
        sum += v;
      }
      return ImmutableList.of(sum);
    }
  };
  
  public static Aggregator<Integer> SUM_INTS = new Aggregator<Integer>() {
    @Override
    public Iterable<Integer> apply(Iterable<Integer> next) {
      int sum = 0;
      for (Integer v : next) {
        sum += v;
      }
      return ImmutableList.of(sum);
    }
  };
  
  public static Aggregator<Float> SUM_FLOATS = new Aggregator<Float>() {
    @Override
    public Iterable<Float> apply(Iterable<Float> next) {
      float sum = 0f;
      for (Float v : next) {
        sum += v;
      }
      return ImmutableList.of(sum);
    }
  };
  
  public static Aggregator<Double> SUM_DOUBLES = new Aggregator<Double>() {
    @Override
    public Iterable<Double> apply(Iterable<Double> next) {
      double sum = 0;
      for (Double v : next) {
        sum += v;
      }
      return ImmutableList.of(sum);
    }
  };

  public static Aggregator<Long> MAX_LONGS = new Aggregator<Long>() {
    @Override
    public Iterable<Long> apply(Iterable<Long> next) {
      Long max = null;
      for (Long v : next) {
        if (max == null || max < v) {
          max = v;
        }
      }
      return ImmutableList.of(max);
    }
  };
  
  public static Aggregator<Integer> MAX_INTS = new Aggregator<Integer>() {
    @Override
    public Iterable<Integer> apply(Iterable<Integer> next) {
      Integer max = null;
      for (Integer v : next) {
        if (max == null || max < v) {
          max = v;
        }
      }
      return ImmutableList.of(max);
    }
  };
  
  public static Aggregator<Float> MAX_FLOATS = new Aggregator<Float>() {
    @Override
    public Iterable<Float> apply(Iterable<Float> next) {
      Float max = null;
      for (Float v : next) {
        if (max == null || max < v) {
          max = v;
        }
      }
      return ImmutableList.of(max);
    }
  };
  
  public static Aggregator<Double> MAX_DOUBLES = new Aggregator<Double>() {
    @Override
    public Iterable<Double> apply(Iterable<Double> next) {
      Double max = null;
      for (Double v : next) {
        if (max == null || max < v) {
          max = v;
        }
      }
      return ImmutableList.of(max);
    }
  };
  
  public static Aggregator<Long> MIN_LONGS = new Aggregator<Long>() {
    @Override
    public Iterable<Long> apply(Iterable<Long> next) {
      Long min = null;
      for (Long v : next) {
        if (min == null || min > v) {
          min = v;
        }
      }
      return ImmutableList.of(min);
    }
  };
  
  public static Aggregator<Integer> MIN_INTS = new Aggregator<Integer>() {
    @Override
    public Iterable<Integer> apply(Iterable<Integer> next) {
      Integer min = null;
      for (Integer v : next) {
        if (min == null || min > v) {
          min = v;
        }
      }
      return ImmutableList.of(min);
    }
  };
  
  public static Aggregator<Float> MIN_FLOATS = new Aggregator<Float>() {
    @Override
    public Iterable<Float> apply(Iterable<Float> next) {
      Float min = null;
      for (Float v : next) {
        if (min == null || min > v) {
          min = v;
        }
      }
      return ImmutableList.of(min);
    }
  };
  
  public static Aggregator<Double> MIN_DOUBLES = new Aggregator<Double>() {
    @Override
    public Iterable<Double> apply(Iterable<Double> next) {
      Double min = null;
      for (Double v : next) {
        if (min == null || min > v) {
          min = v;
        }
      }
      return ImmutableList.of(min);
    }
  };

  public static class MaxNAggregator<V> implements Aggregator<V> {
    private final int arity;
    private final Comparator<V> cmp;
    
    public MaxNAggregator(int arity, Comparator<V> cmp) {
      this.arity = arity;
      this.cmp = cmp;
    }

    @Override
    public Iterable<V> apply(Iterable<V> values) {
      SortedSet<V> current = Sets.newTreeSet(cmp);
      for (V value : values) {
        if (current.size() < arity) {
          current.add(value);
        } else if (cmp.compare(value, current.first()) > 0) {
          current.remove(current.first());
          current.add(value);
        }
      }
      return current;
    }
  }
  
  public static class MinNAggregator<V> implements Aggregator<V> {
    private final int arity;
    private final Comparator<V> cmp;
    
    public MinNAggregator(int arity, Comparator<V> cmp) {
      this.arity = arity;
      this.cmp = cmp;
    }

    @Override
    public Iterable<V> apply(Iterable<V> values) {
      SortedSet<V> current = Sets.newTreeSet(cmp);
      for (V value : values) {
        if (current.size() < arity) {
          current.add(value);
        } else if (cmp.compare(value, current.last()) < 0) {
          current.remove(current.last());
          current.add(value);
        }
      }
      return current;
    }
  }
  
  public static class FirstNAggregator<V> implements Aggregator<V> {
    private final int arity;
    
    public FirstNAggregator(int arity) {
      this.arity = arity;
    }
    
    @Override
    public Iterable<V> apply(Iterable<V> values) {
      List<V> kept = Lists.newArrayList();
      for (V value : values) {
        kept.add(value);
        if (kept.size() == arity) {
          break;
        }
      }
      return kept;
    }
  }

  public static class LastNAggregator<V> implements Aggregator<V> {
    private final int arity;
    
    public LastNAggregator(int arity) {
      this.arity = arity;
    }
    
    @Override
    public Iterable<V> apply(Iterable<V> values) {
      LinkedList<V> kept = Lists.newLinkedList();
      for (V value : values) {
        kept.add(value);
        if (kept.size() == arity + 1) {
          kept.removeFirst();
        }
      }
      return kept;
    }
  }

}
