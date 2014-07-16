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
print "import org.apache.crunch.TupleN"
print "import org.apache.crunch.types.PType\n"

print "trait GeneratedTuplePTypeFamily extends BasePTypeFamily {"
print "  import GeneratedTupleHelper._\n"
for j in range(5, 23):
  lets = letters[0:j]
  types = ", ".join(lets)
  args = ", ".join(["p%d: PType[%s]" % (x, l) for (x, l) in enumerate(lets)])
  print "  def tuple%d[%s](%s) = {" % (j, types, args)

  inout = ",".join(["t.get(%d).asInstanceOf[%s]" % (x, l) for (x, l) in enumerate(lets)])
  print "    val in = (t: TupleN) => (%s)" % inout

  outin = ", ".join(lets)
  outout = ", ".join(["t._%d" % (1 + x) for x in range(j)])
  print "    val out = (t: (%s)) => tupleN(%s)" % (outin, outout)
  derout = ", ".join(["p%d" % x for x in range(j)])
  print "    derived(classOf[(%s)], in, out, ptf.tuples(%s))" % (outin, derout)
  print "  }\n"
print "}\n"
