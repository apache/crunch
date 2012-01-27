#!/usr/bin/python

import os
import re
import shutil
import subprocess
import sys

# Configuration in script
##############################################################
SCALA_HOME="/Users/josh/scala/scala-2.9.1.final/lib"
HADOOP_JAR="/Users/josh/cdh/hadoop-0.20.2-cdh3u1/hadoop-core-0.20.2-cdh3u1.jar"

#Get the absolute path of the original (non-symlink) file.
if os.path.islink(__file__):
  ORIGINAL_FILE = os.readlink(__file__)
else:
  ORIGINAL_FILE = __file__

SCIENCE_ROOT = os.path.abspath(os.path.dirname(ORIGINAL_FILE)+"/../")
JARFILE = SCIENCE_ROOT + "/target/scrunch-0.1.0-jar-with-dependencies.jar" #what jar has all the depencies for this job
print JARFILE
TMPDIR = "/tmp"
BUILDDIR = TMPDIR + "/script-build"
COMPILE_CMD = "java -cp %s/scala-library.jar:%s/scala-compiler.jar -Dscala.home=%s scala.tools.nsc.Main" % (SCALA_HOME, SCALA_HOME, SCALA_HOME)
##############################################################

argv = sys.argv[1:]
if len(argv) < 1:
  sys.stderr.write("ERROR: insufficient args.\n")
  sys.exit(1)

JOBFILE = argv.pop(0)

def file_type():
  m = re.search(r'\.(scala|java)$', JOBFILE)
  if m:
    return m.group(1)
  return None

def is_file():
  return file_type() is not None

PACK_RE = r'package ([^;]+)'
OBJECT_RE = r'object\s+([^\s(]+).*(extends|with)\s+PipelineApp.*'
EXTENSION_RE = r'(.*)\.(scala|java)$'

#Get the name of the job from the file.
#the rule is: last class in the file, or the one that matches the filename
def get_job_name(file):
  package = ""
  job = None
  default = None
  match = re.search(EXTENSION_RE, file)
  if match:
    default = match.group(1)
    for s in open(file, "r"):
      mp = re.search(PACK_RE, s)
      mo = re.search(OBJECT_RE, s)
      if mp:
        package = mp.group(1).trim() + "."
      elif mo:
        if not job or not default or not job.tolower() == default.tolower():
          #use either the last class, or the one with the same name as the file
          job = mo.group(1)
    if not job:
      raise "Could not find job name"
    return "%s%s" % (package, job)
  else:
    return file

JARPATH = os.path.abspath(JARFILE)
JARBASE = os.path.basename(JARFILE)
JOBPATH = os.path.abspath(JOBFILE)
JOB = get_job_name(JOBFILE)
JOBJAR = JOB + ".jar"
JOBJARPATH = os.path.join(TMPDIR, JOBJAR)

def needs_rebuild():
  return not os.path.exists(JOBJARPATH) or os.stat(JOBJARPATH).st_mtime < os.stat(JOBPATH).st_mtime

def build_job_jar():
  sys.stderr.write("compiling " + JOBFILE + "\n")
  if os.path.exists(BUILDDIR):
    shutil.rmtree(BUILDDIR)
  os.makedirs(BUILDDIR)
  cmd = "%s -classpath %s:%s -d %s %s" % (COMPILE_CMD, JARPATH, HADOOP_JAR, BUILDDIR, JOBFILE)
  print cmd
  if subprocess.call(cmd, shell=True):
    shutil.rmtree(BUILDDIR)
    sys.exit(1)

  shutil.copy(JARPATH, JOBJARPATH)
  jar_cmd = "jar uf %s -C %s ." % (JOBJARPATH, BUILDDIR)
  subprocess.call(jar_cmd, shell=True)
  shutil.rmtree(BUILDDIR)

def hadoop_command():
  return "hadoop jar %s %s %s" % (JOBJARPATH, JOB, " ".join(argv))

if is_file() and needs_rebuild():
  build_job_jar()

SHELL_COMMAND = hadoop_command()
print SHELL_COMMAND
os.system(SHELL_COMMAND)
