package org.apache.crunch.test;

import org.apache.crunch.MapFn;

/**
 * Simple String wrapper for testing with Avro reflection.
 */
public class StringWrapper implements Comparable<StringWrapper> {

  public static class StringToStringWrapperMapFn extends MapFn<String, StringWrapper> {

    @Override
    public StringWrapper map(String input) {
      return wrap(input);
    }

  }

  public static class StringWrapperToStringMapFn extends MapFn<StringWrapper, String> {

    @Override
    public String map(StringWrapper input) {
      return input.getValue();
    }

  }

  private String value;

  public StringWrapper() {
    this("");
  }

  public StringWrapper(String value) {
    this.value = value;
  }

  @Override
  public int compareTo(StringWrapper o) {
    return this.value.compareTo(o.value);
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    StringWrapper other = (StringWrapper) obj;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "StringWrapper [value=" + value + "]";
  }

  public static StringWrapper wrap(String value) {
    return new StringWrapper(value);
  }

}
