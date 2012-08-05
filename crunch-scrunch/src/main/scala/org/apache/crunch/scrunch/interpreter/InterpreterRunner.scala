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
package org.apache.crunch.scrunch.interpreter

import java.io.File
import java.io.FileOutputStream
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

import scala.tools.nsc.GenericRunnerCommand
import scala.tools.nsc.Global
import scala.tools.nsc.MainGenericRunner
import scala.tools.nsc.ObjectRunner
import scala.tools.nsc.Properties
import scala.tools.nsc.ScriptRunner
import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.io.Jar
import scala.tools.nsc.io.VirtualDirectory

import com.google.common.io.Files
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration

import org.apache.crunch.util.DistCache

/**
 * An object used to run a Scala REPL with modifications to facilitate Scrunch jobs running
 * within the REPL.
 */
object InterpreterRunner extends MainGenericRunner {

  // The actual Scala repl.
  var repl: ILoop = null

  /**
   * Checks whether or not the Scala repl has been started.
   *
   * @return <code>true</code> if the repl is running, <code>false</code> otherwise.
   */
  def isReplRunning() = repl == null

  /**
   * The main entry point for the REPL.  This method is lifted from
   * {@link scala.tools.nsc.MainGenericRunner} and modified to facilitate testing whether or not
   * the REPL is actually running.
   *
   * @param args Arguments used on the command line to start the REPL.
   * @return <code>true</code> if execution was successful, <code>false</code> otherwise.
   */
  override def process(args: Array[String]): Boolean = {
    val command = new GenericRunnerCommand(args.toList, (x: String) => errorFn(x))
    import command.{settings, howToRun, thingToRun}
    // Defines a nested function to retrieve a sample compiler if necessary.
    def sampleCompiler = new Global(settings)

    import Properties.{versionString, copyrightString}
    if (!command.ok) {
      return errorFn("\n" + command.shortUsageMsg)
    } else if (settings.version.value) {
      return errorFn("Scala code runner %s -- %s".format(versionString, copyrightString))
    } else if (command.shouldStopWithInfo) {
      return errorFn(command getInfoMessage sampleCompiler)
    }

    // Functions to retrieve settings values that were passed to REPL invocation.
    // The -e argument provides a Scala statement to execute.
    // The -i option requests that a file be preloaded into the interactive shell.
    def isE = !settings.execute.isDefault
    def dashe = settings.execute.value
    def isI = !settings.loadfiles.isDefault
    def dashi = settings.loadfiles.value

    // Function to retrieve code passed by -e and -i options to REPL.
    def combinedCode = {
      val files = if (isI) dashi map (file => scala.tools.nsc.io.File(file).slurp()) else Nil
      val str = if (isE) List(dashe) else Nil
      files ++ str mkString "\n\n"
    }

    import GenericRunnerCommand._

    // Function for running the target command. It can run an object with main, a script, or
    // an interactive REPL.
    def runTarget(): Either[Throwable, Boolean] = howToRun match {
      case AsObject =>
        ObjectRunner.runAndCatch(settings.classpathURLs, thingToRun, command.arguments)
      case AsScript =>
        ScriptRunner.runScriptAndCatch(settings, thingToRun, command.arguments)
      case AsJar =>
        ObjectRunner.runAndCatch(
          scala.tools.nsc.io.File(thingToRun).toURL +: settings.classpathURLs,
          new Jar(thingToRun).mainClass getOrElse sys.error("Cannot find main class for jar: " +
            thingToRun),
          command.arguments
        )
      case Error =>
        Right(false)
      case _ =>
        // We start the shell when no arguments are given.
        repl = new ILoop
        Right(repl.process(settings))
    }

    /**If -e and -i were both given, we want to execute the -e code after the
     *  -i files have been included, so they are read into strings and prepended to
     *  the code given in -e.  The -i option is documented to only make sense
     *  interactively so this is a pretty reasonable assumption.
     *
     *  This all needs a rewrite though.
     */
    if (isE) {
      ScriptRunner.runCommand(settings, combinedCode, thingToRun +: command.arguments)
    }
    else runTarget() match {
      case Left(ex) => errorFn(ex)
      case Right(b) => b
    }
  }

  def main(args: Array[String]) {
    val retVal = process(args)
    if (!retVal)
      sys.exit(1)
  }

  /**
   * Creates a jar file containing the code thus far compiled by the REPL in a temporary directory.
   *
   * @return A file object representing the jar file created.
   */
  def createReplCodeJar(): File = {
    var jarStream: JarOutputStream = null
    try {
      val virtualDirectory = repl.virtualDirectory
      val tempDir = Files.createTempDir()
      val tempJar = new File(tempDir, "replJar.jar")
      jarStream = new JarOutputStream(new FileOutputStream(tempJar))
      addVirtualDirectoryToJar(virtualDirectory, "", jarStream)
      return tempJar
    } finally {
      IOUtils.closeQuietly(jarStream)
    }
  }

  /**
   * Add the contents of the specified virtual directory to a jar. This method will recursively
   * descend into subdirectories to add their contents.
   *
   * @param dir The virtual directory whose contents should be added.
   * @param entryPath The entry path for classes found in the virtual directory.
   * @param jarStream An output stream for writing the jar file.
   */
  def addVirtualDirectoryToJar(dir: VirtualDirectory, entryPath: String, jarStream:
      JarOutputStream): Unit = {
    dir.foreach { file =>
      if (file.isDirectory) {
        // Recursively descend into subdirectories, adjusting the package name as we do.
        val dirPath = entryPath + file.name + "/"
        val entry: JarEntry = new JarEntry(dirPath)
        jarStream.putNextEntry(entry)
        jarStream.closeEntry()
        addVirtualDirectoryToJar(file.asInstanceOf[VirtualDirectory],
            dirPath, jarStream)
      } else if (file.hasExtension("class")) {
        // Add class files as an entry in the jar file and write the class to the jar.
        val entry: JarEntry = new JarEntry(entryPath + file.name)
        jarStream.putNextEntry(entry)
        jarStream.write(file.toByteArray)
        jarStream.closeEntry()
      }
    }
  }

  /**
   * Generates a jar containing the code thus far compiled by the REPL,
   * and adds that jar file to the distributed cache of jobs using the specified configuration.
   * Also adds any jars added with the :cp command to the user's job.
   *
   * @param configuration The configuration of jobs that should use the REPL code jar.
   */
  def addReplJarsToJob(configuration: Configuration): Unit = {
    if (repl != null) {
      // Generate a jar of REPL code and add to the distributed cache.
      val replJarFile = createReplCodeJar()
      DistCache.addJarToDistributedCache(configuration, replJarFile)
      // Get the paths to jars added with the :cp command.
      val addedJarPaths = repl.addedClasspath.split(':')
      addedJarPaths.foreach {
        path => if (path.endsWith(".jar")) DistCache.addJarToDistributedCache(configuration, path)
      }
    }
  }
}
