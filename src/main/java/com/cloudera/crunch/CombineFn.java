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

/**
 * A special {@link DoFn} implementation that converts an {@link Iterable}
 * of values into a single value. If a {@code CombineFn} instance is used
 * on a {@link PGroupedTable}, the function will be applied to the output
 * of the map stage before the data is passed to the reducer, which can
 * improve the runtime of certain classes of jobs.
 *
 */
public abstract class CombineFn<S, T> extends DoFn<Pair<S, Iterable<T>>, Pair<S, T>> {

  /**
   * The associative and commutative combiner function.
   * 
   * @param values the values to combine
   * @return the combined result of the inputs
   */
  public abstract T combine(Iterable<T> values);

  @Override
  public void process(Pair<S, Iterable<T>> input, Emitter<Pair<S, T>> emitter) {
    emitter.emit(Pair.of(input.first(), combine(input.second())));
  }

  @Override
  public float scaleFactor() {
    return 0.9f;
  }

  public static final <K> CombineFn<K, Long> SUM_LONGS() {
    return new CombineFn<K, Long>() {
      @Override
      public Long combine(Iterable<Long> values) {
        long sum = 0L;
        for (Long v : values) {
          sum += v;
        }
        return sum;
      }
    };
  }

  public static final <K> CombineFn<K, Integer> SUM_INTS() {
    return new CombineFn<K, Integer>() {
      @Override
      public Integer combine(Iterable<Integer> values) {
        int sum = 0;
        for (Integer v : values) {
          sum += v;
        }
        return sum;
      }
    };
  }

  public static final <K> CombineFn<K, Float> SUM_FLOATS() {
    return new CombineFn<K, Float>() {
      @Override
      public Float combine(Iterable<Float> values) {
        float sum = 0f;
        for (Float v : values) {
          sum += v;
        }
        return sum;
      }
    };
  }

  public static final <K> CombineFn<K, Double> SUM_DOUBLES() {
    return new CombineFn<K, Double>() {
      @Override
      public Double combine(Iterable<Double> values) {
        double sum = 0;
        for (Double v : values) {
          sum += v;
        }
        return sum;
      }
    };
  }

  public static final <K> CombineFn<K, Long> MAX_LONGS() {
    return new CombineFn<K, Long>() {
      @Override
      public Long combine(Iterable<Long> values) {
        Long max = null;
        for (Long value : values) {
          if (max == null || value > max) {
            max = value;
          }
        }
        return max;
      }
    };
  }

  public static final <K> CombineFn<K, Integer> MAX_INTS() {
    return new CombineFn<K, Integer>() {
      @Override
      public Integer combine(Iterable<Integer> values) {
        Integer max = null;
        for (Integer value : values) {
          if (max == null || value > max) {
            max = value;
          }
        }
        return max;
      }
    };
  }

  public static final <K> CombineFn<K, Float> MAX_FLOATS() {
    return new CombineFn<K, Float>() {
      @Override
      public Float combine(Iterable<Float> values) {
        Float max = null;
        for (Float value : values) {
          if (max == null || value > max) {
            max = value;
          }
        }
        return max;
      }
    };
  }

  public static final <K> CombineFn<K, Double> MAX_DOUBLES() {
    return new CombineFn<K, Double>() {
      @Override
      public Double combine(Iterable<Double> values) {
        Double max = null;
        for (Double value : values) {
          if (max == null || value > max) {
            max = value;
          }
        }
        return max;
      }
    };
  }

  public static final <K> CombineFn<K, Long> MIN_LONGS() {
    return new CombineFn<K, Long>() {
      @Override
      public Long combine(Iterable<Long> values) {
        Long min = null;
        for (Long value : values) {
          if (min == null || value < min) {
            min = value;
          }
        }
        return min;
      }
    };
  }

  public static final <K> CombineFn<K, Integer> MIN_INTS() {
    return new CombineFn<K, Integer>() {
      @Override
      public Integer combine(Iterable<Integer> values) {
        Integer min = null;
        for (Integer value : values) {
          if (min == null || value < min) {
            min = value;
          }
        }
        return min;
      }
    };
  }

  public static final <K> CombineFn<K, Float> MIN_FLOATS() {
    return new CombineFn<K, Float>() {
      @Override
      public Float combine(Iterable<Float> values) {
        Float min = null;
        for (Float value : values) {
          if (min == null || value < min) {
            min = value;
          }
        }
        return min;
      }
    };
  }

  public static final <K> CombineFn<K, Double> MIN_DOUBLES() {
    return new CombineFn<K, Double>() {
      @Override
      public Double combine(Iterable<Double> values) {
        Double min = null;
        for (Double value : values) {
          if (min == null || value < min) {
            min = value;
          }
        }
        return min;
      }
    };
  }
}
