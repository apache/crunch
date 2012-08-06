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
package org.apache.scrunch.interpreter

import java.io.File
import java.io.FileOutputStream
import java.util.jar.JarFile
import java.util.jar.JarOutputStream

import scala.tools.nsc.io.VirtualDirectory

import com.google.common.io.Files
import org.junit.Assert.assertNotNull
import org.junit.Test
import org.apache.crunch.test.CrunchTestSupport
import org.scalatest.junit.JUnitSuite

/**
 * Tests creating jars from a {@link scala.tools.nsc.io.VirtualDirectory}.
 */
class InterpreterJarTest extends CrunchTestSupport with JUnitSuite {

  /**
   * Tests transforming a virtual directory into a temporary jar file.
   */
  @Test def virtualDirToJar: Unit = {
    // Create a virtual directory and populate with some mock content.
    val root = new VirtualDirectory("testDir", None)
    // Add some subdirectories to the root.
    (1 to 10).foreach { i =>
      val subdir = root.subdirectoryNamed("subdir" + i).asInstanceOf[VirtualDirectory]
      // Add some classfiles to each sub directory.
      (1 to 10).foreach { j =>
        subdir.fileNamed("MyClass" + j + ".class")
      }
    }

    // Now generate a jar file from the virtual directory.
    val tempJar = new File(tempDir.getRootFile(), "replJar.jar")
    val jarStream = new JarOutputStream(new FileOutputStream(tempJar))
    InterpreterRunner.addVirtualDirectoryToJar(root, "top/pack/name/", jarStream)
    jarStream.close()

    // Verify the contents of the jar.
    val jarFile = new JarFile(tempJar)
    (1 to 10).foreach { i =>
      (1 to 10).foreach { j =>
        val entryName = "top/pack/name/subdir" + i + "/MyClass" + j + ".class"
        val entry = jarFile.getEntry(entryName)
        assertNotNull("Jar entry " + entryName + " not found in generated jar.", entry)
      }
    }
  }
}
