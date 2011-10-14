package com.cloudera.crunch.type;

import com.cloudera.crunch.*;

import java.io.Serializable;

public abstract class Tuplifier implements Serializable {

  public abstract Tuple tuplify(Object... values);

  public Tuplifier() {}

  public static final Tuplifier PAIR = new Tuplifier() {
    @Override
    public Tuple tuplify(Object... values) {
      if (values == null) return Pair.of(null, null);
      if (values.length < 1) return Pair.of(null, null);
      if (values.length < 2) return Pair.of(values[0], null);
      else return Pair.of(values[0], values[1]);
    }
  };

  public static final Tuplifier TUPLE3 = new Tuplifier() {
    @Override
    public Tuple tuplify(Object... values) {
      if (values == null || values.length < 1) return new Tuple3(null, null, null);
      else if (values.length < 2) return new Tuple3(values[0], null, null);
      else if (values.length < 3) return new Tuple3(values[0], values[1], null);
      else return new Tuple3(values[0], values[1], values[2]);
    }
  };

  public static final Tuplifier TUPLE4 = new Tuplifier() {
    @Override
    public Tuple tuplify(Object... values) {
      if (values == null || values.length < 1) return new Tuple4(null, null, null, null);
      else if (values.length < 2) return new Tuple4(values[0], null, null, null);
      else if (values.length < 3) return new Tuple4(values[0], values[1], null, null);
      else if (values.length < 3) return new Tuple4(values[0], values[1], values[2], null);
      else return new Tuple4(values[0], values[1], values[2], values[3]);
    }
  };

  public static final Tuplifier TUPLEN = new Tuplifier() {
    @Override
    public Tuple tuplify(Object... values) {
      return new TupleN(values);
    }
  };

}
