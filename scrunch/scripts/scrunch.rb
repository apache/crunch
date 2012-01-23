#!/usr/bin/env ruby
require 'fileutils'
require 'thread'

#Usage : scrunch.rb [--hdfs|--local|--print] job <job args>
# --hdfs: if job ends in ".scala" or ".java" and the file exists, link it against JARFILE (below) and then run it on HOST.
#         else, it is assumed to be a full classname to an item in the JARFILE, which is run on HOST
# --local: run locally according to the rules above
# --print: print the command YOU SHOULD ENTER on the remote node. Useful for screen sessions.

# Configuration in script
##############################################################
SCALA_HOME="/Users/josh/scala/scala-2.9.1.final/lib"
HADOOP_JAR="/Users/josh/cdh/hadoop-0.20.2-cdh3u1/hadoop-core-0.20.2-cdh3u1.jar"

#Get the absolute path of the original (non-symlink) file.
ORIGINAL_FILE=File.symlink?(__FILE__) ? File.readlink(__FILE__) : __FILE__
SCIENCE_ROOT=File.expand_path(File.dirname(ORIGINAL_FILE)+"/../")
JARFILE=SCIENCE_ROOT + "/target/scrunch-1.0-SNAPSHOT-jar-with-dependencies.jar" #what jar has all the depencies for this job
puts JARFILE
TMPDIR="/tmp"
BUILDDIR=TMPDIR+"/script-build"
COMPILE_CMD="java -cp #{SCALA_HOME}/scala-library.jar:#{SCALA_HOME}/scala-compiler.jar -Dscala.home=#{SCALA_HOME} scala.tools.nsc.Main"
##############################################################

if ARGV.size < 1
  $stderr.puts("ERROR: insufficient args.")
  #Make sure to print out up to Configuration above:
  system("head -n 19 #{__FILE__} | tail -n+4")
  exit(1)
end

MODE = case ARGV[0]
  when "--hdfs"
    ARGV.shift
  when "--print"
    ARGV.shift
  else
    #default:
    "--hdfs"
end

JOBFILE=ARGV.shift

def file_type
  JOBFILE =~ /\.(scala|java)$/
  $1
end

def is_file?
  !file_type.nil?
end

PACK_RE = /^package ([^;]+)/
OBJECT_RE = /object\s+([^\s(]+).*/
EXTENSION_RE = /(.*)\.(scala|java)$/

#Get the name of the job from the file.
#the rule is: last class in the file, or the one that matches the filename
def get_job_name(file)
  package = ""
  job = nil
  default = nil
  if file =~ EXTENSION_RE
    default = $1
    File.readlines(file).each { |s|
      if s =~ PACK_RE
        package = $1.chop + "."
      elsif s =~ OBJECT_RE
        unless job and default and (job.downcase == default.downcase)
          #use either the last class, or the one with the same name as the file
          job = $1
        end
      end
    }
    raise "Could not find job name" unless job
    "#{package}#{job}"
  else
    file
  end
end

JARPATH=File.expand_path(JARFILE)
JARBASE=File.basename(JARFILE)
JOBPATH=File.expand_path(JOBFILE)
JOB=get_job_name(JOBFILE)
JOBJAR=JOB+".jar"
JOBJARPATH=TMPDIR+"/"+JOBJAR

def needs_rebuild?
  !File.exists?(JOBJARPATH) || File.stat(JOBJARPATH).mtime < File.stat(JOBPATH).mtime
end

def build_job_jar
  $stderr.puts("compiling " + JOBFILE)
  FileUtils.mkdir_p(BUILDDIR)
  unless system("#{COMPILE_CMD} -classpath #{JARPATH}:#{HADOOP_JAR} -d #{BUILDDIR} #{JOBFILE}")
    FileUtils.rm_rf(BUILDDIR)
    exit(1)
  end

  FileUtils.rm_f(JOBJARPATH)
  system("cp #{JARPATH} #{JOBJARPATH}")
  system("jar uf #{JOBJARPATH} -C #{BUILDDIR} .")
  # FileUtils.rm_rf(BUILDDIR)
end

def hadoop_command
  "hadoop jar #{JOBJARPATH} #{JOB} " + ARGV.join(" ")
end

def jar_mode_command
  "hadoop jar #{JOBJARPATH} #{JOB} " + ARGV.join(" ")
end

if is_file?
  build_job_jar if needs_rebuild?
end

SHELL_COMMAND = case MODE
  when "--hdfs"
    if is_file?
      "#{hadoop_command}"
    else
      "#{jar_mode_command}"
    end
  when "--print"
    if is_file?
      "echo #{hadoop_command}"
    else
      "echo #{jar_mode_command}"
    end
  else
    raise "Unrecognized mode: " + MODE
  end
puts SHELL_COMMAND
system(SHELL_COMMAND)
