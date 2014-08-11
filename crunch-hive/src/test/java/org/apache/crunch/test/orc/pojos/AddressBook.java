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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class AddressBook {
  
  public static final String TYPE_STR = "struct<myname:string,mynumbers:array<string>,"
      + "contacts:map<string," + Person.TYPE_STR + ">,updatetime:timestamp,signature:binary>";
  public static final TypeInfo TYPE_INFO = TypeInfoUtils.getTypeInfoFromTypeString(TYPE_STR);
  
  private String myName;
  
  private List<String> myNumbers;
  private Map<String, Person> contacts;
  
  private Timestamp updateTime;
  private byte[] signature;
  
  public AddressBook() {}

  public AddressBook(String myName, List<String> myNumbers,
      Map<String, Person> contacts, Timestamp updateTime, byte[] signature) {
    super();
    this.myName = myName;
    this.myNumbers = myNumbers;
    this.contacts = contacts;
    this.updateTime = updateTime;
    this.signature = signature;
  }

  public String getMyName() {
    return myName;
  }

  public void setMyName(String myName) {
    this.myName = myName;
  }

  public List<String> getMyNumbers() {
    return myNumbers;
  }

  public void setMyNumbers(List<String> myNumbers) {
    this.myNumbers = myNumbers;
  }

  public Map<String, Person> getContacts() {
    return contacts;
  }

  public void setContacts(Map<String, Person> contacts) {
    this.contacts = contacts;
  }

  public Timestamp getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Timestamp updateTime) {
    this.updateTime = updateTime;
  }

  public byte[] getSignature() {
    return signature;
  }

  public void setSignature(byte[] signature) {
    this.signature = signature;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((contacts == null) ? 0 : contacts.hashCode());
    result = prime * result + ((myName == null) ? 0 : myName.hashCode());
    result = prime * result + ((myNumbers == null) ? 0 : myNumbers.hashCode());
    result = prime * result + Arrays.hashCode(signature);
    result = prime * result
        + ((updateTime == null) ? 0 : updateTime.hashCode());
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
    AddressBook other = (AddressBook) obj;
    if (contacts == null) {
      if (other.contacts != null)
        return false;
    } else if (!contacts.equals(other.contacts))
      return false;
    if (myName == null) {
      if (other.myName != null)
        return false;
    } else if (!myName.equals(other.myName))
      return false;
    if (myNumbers == null) {
      if (other.myNumbers != null)
        return false;
    } else if (!myNumbers.equals(other.myNumbers))
      return false;
    if (!Arrays.equals(signature, other.signature))
      return false;
    if (updateTime == null) {
      if (other.updateTime != null)
        return false;
    } else if (!updateTime.equals(other.updateTime))
      return false;
    return true;
  }

}
