##
## Licensed to the Apache Software Foundation (ASF) under one
## or more contributor license agreements.  See the NOTICE file
## distributed with this work for additional information
## regarding copyright ownership.  The ASF licenses this file
## to you under the Apache License, Version 2.0 (the
## "License"); you may not use this file except in compliance
## with the License.  You may obtain a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing,
## software distributed under the License is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied.  See the License for the
## specific language governing permissions and limitations
## under the License.
##
#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package};

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.is;
import org.apache.crunch.FilterFn;
import org.junit.Test;


public class StopWordFilterTest {

  @Test
  public void testFilter() {
    FilterFn<String> filter = new StopWordFilter();

    assertThat(filter.accept("foo"), is(true));
    assertThat(filter.accept("the"), is(false));
    assertThat(filter.accept("a"), is(false));
  }

}
