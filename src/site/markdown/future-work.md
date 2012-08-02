<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Current Limitations and Future Work
---

This section contains an almost certainly incomplete list of known limitations of Crunch and plans for future work.

* We would like to have easy support for reading and writing data from/to HCatalog.
* The decision of how to split up processing tasks between dependent MapReduce jobs is very naiive right now- we simply
delegate all of the work to the reduce stage of the predecessor job. We should take advantage of information about the
expected size of different PCollections to optimize this processing.
* The Crunch optimizer does not yet merge different groupByKey operations that run over the same input data into a single
MapReduce job. Implementing this optimization will provide a major performance benefit for a number of problems.
