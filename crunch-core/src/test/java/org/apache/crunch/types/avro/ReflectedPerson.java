/*
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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.crunch.types.avro;

import java.util.List;

/**
 * A test helper class that conforms to the Person Avro specific data class, to use the Person schema for testing
 * with reflection-based reading and writing.
 */
public class ReflectedPerson {

  private String name;
  private int age;
  private List<String> siblingnames;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public List<String> getSiblingnames() {
    return siblingnames;
  }

  public void setSiblingnames(List<String> siblingnames) {
    this.siblingnames = siblingnames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ReflectedPerson that = (ReflectedPerson) o;

    if (age != that.age) return false;
    if (name != null ? !name.equals(that.name) : that.name != null) return false;
    if (siblingnames != null ? !siblingnames.equals(that.siblingnames) : that.siblingnames != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + age;
    result = 31 * result + (siblingnames != null ? siblingnames.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ReflectedPerson{" +
        "name='" + name + '\'' +
        ", age=" + age +
        ", siblingnames=" + siblingnames +
        '}';
  }
}
