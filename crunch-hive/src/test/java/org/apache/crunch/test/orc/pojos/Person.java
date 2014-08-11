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
package org.apache.crunch.test.orc.pojos;

import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class Person {
  
  public static final String TYPE_STR = "struct<name:string,age:int,number:array<string>>";
  public static final TypeInfo TYPE_INFO = TypeInfoUtils.getTypeInfoFromTypeString(TYPE_STR);
  
  private String name;
  private int age;
  private List<String> numbers;

  public Person() {}
  
  public Person(String name, int age, List<String> numbers) {
    super();
    this.name = name;
    this.age = age;
    this.numbers = numbers;
  }

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

  public List<String> getNumbers() {
    return numbers;
  }

  public void setNumbers(List<String> numbers) {
    this.numbers = numbers;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + age;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((numbers == null) ? 0 : numbers.hashCode());
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
    Person other = (Person) obj;
    if (age != other.age)
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (numbers == null) {
      if (other.numbers != null)
        return false;
    } else if (!numbers.equals(other.numbers))
      return false;
    return true;
  }

}
