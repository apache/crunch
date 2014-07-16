#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

print """/**
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
 */"""
print "package org.apache.crunch.scrunch\n"
print "trait GeneratedTupleConversions {"
for j in range(5, 23):
  lets = letters[0: j]
  types = ", ".join(lets)
  pth = ", ".join(["%s: PTypeH" % x for x in lets])
  print "  implicit def tuple%d[%s] = new PTypeH[(%s)] {" % (j, pth, types)
  print "    def get(ptf: PTypeFamily) = {"
  implicits = ", ".join(["implicitly[PTypeH[%s]].get(ptf)" % x for x in lets])
  print "      ptf.tuple%d(%s)" % (j, implicits)
  print "    }"
  print "  }\n"
print "}"
