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
package org.apache.crunch.types;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;

import com.google.common.collect.Maps;

public abstract class TupleFactory<T extends Tuple> implements Serializable {

  public void initialize() {
  }

  public abstract T makeTuple(Object... values);

  
  private static final Map<Class, TupleFactory> customTupleFactories = Maps.newHashMap();
  
  /**
   * Get the {@link TupleFactory} for a given Tuple implementation.
   * 
   * @param tupleClass
   *          The class for which the factory is to be retrieved
   * @return The appropriate TupleFactory
   */
  public static <T extends Tuple> TupleFactory<T> getTupleFactory(Class<T> tupleClass) {
    if (tupleClass == Pair.class) {
      return (TupleFactory<T>) PAIR;
    } else if (tupleClass == Tuple3.class) {
      return (TupleFactory<T>) TUPLE3;
    } else if (tupleClass == Tuple4.class) {
      return (TupleFactory<T>) TUPLE4;
    } else if (tupleClass == TupleN.class) {
      return (TupleFactory<T>) TUPLEN;
    } else if (customTupleFactories.containsKey(tupleClass)) {
      return (TupleFactory<T>) customTupleFactories.get(tupleClass);
    } else {
      throw new IllegalArgumentException("Can't create TupleFactory for " + tupleClass);
    }
  }

  public static final TupleFactory<Pair> PAIR = new TupleFactory<Pair>() {
    @Override
    public Pair makeTuple(Object... values) {
      return Pair.of(values[0], values[1]);
    }
  };

  public static final TupleFactory<Tuple3> TUPLE3 = new TupleFactory<Tuple3>() {
    @Override
    public Tuple3 makeTuple(Object... values) {
      return Tuple3.of(values[0], values[1], values[2]);
    }
  };

  public static final TupleFactory<Tuple4> TUPLE4 = new TupleFactory<Tuple4>() {
    @Override
    public Tuple4 makeTuple(Object... values) {
      return Tuple4.of(values[0], values[1], values[2], values[3]);
    }
  };

  public static final TupleFactory<TupleN> TUPLEN = new TupleFactory<TupleN>() {
    @Override
    public TupleN makeTuple(Object... values) {
      return new TupleN(values);
    }
  };

  public static <T extends Tuple> TupleFactory<T> create(Class<T> clazz, Class... typeArgs) {
    if (customTupleFactories.containsKey(clazz)) {
      return (TupleFactory<T>) customTupleFactories.get(clazz);
    }
    TupleFactory<T> custom = new CustomTupleFactory<T>(clazz, typeArgs);
    customTupleFactories.put(clazz, custom);
    return custom;
  }

  private static class CustomTupleFactory<T extends Tuple> extends TupleFactory<T> {

    private final Class<T> clazz;
    private final Class[] typeArgs;

    private transient Constructor<T> constructor;

    public CustomTupleFactory(Class<T> clazz, Class[] typeArgs) {
      this.clazz = clazz;
      this.typeArgs = typeArgs;
    }

    @Override
    public void initialize() {
      try {
        constructor = clazz.getConstructor(typeArgs);
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }

    @Override
    public T makeTuple(Object... values) {
      try {
        return constructor.newInstance(values);
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

}
