package com.cloudera.crunch.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.cloudera.crunch.io.text.TextFileReaderFactory;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.writable.Writables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;


public class CompositePathIterableTest {

  
  @Test
  public void testCreate_FilePresent() throws IOException{
    String inputFilePath = FileHelper.createTempCopyOf("set1.txt");
    Configuration conf = new Configuration();
    LocalFileSystem local = FileSystem.getLocal(conf);
    
    Iterable<String> iterable = CompositePathIterable.create(local, new Path(inputFilePath), new TextFileReaderFactory<String>(Writables.strings(), conf));
    
    assertEquals(Lists.newArrayList("b", "c", "a", "e"), Lists.newArrayList(iterable));
    
  }
  
  @Test
  public void testCreate_DirectoryPresentButNoFiles() throws IOException{
    String inputFilePath = Files.createTempDir().getAbsolutePath();
 
    Configuration conf = new Configuration();
    LocalFileSystem local = FileSystem.getLocal(conf);
    
    Iterable<String> iterable = CompositePathIterable.create(local, new Path(inputFilePath), new TextFileReaderFactory<String>(Writables.strings(), conf));
    
    assertTrue(Lists.newArrayList(iterable).isEmpty());
  }
  
  @Test(expected=IOException.class)
  public void testCreate_DirectoryNotPresent() throws IOException{
    File inputFileDir = Files.createTempDir();
    inputFileDir.delete();
    
    // Sanity check
    assertFalse(inputFileDir.exists());
    
    Configuration conf = new Configuration();
    LocalFileSystem local = FileSystem.getLocal(conf);
    
    CompositePathIterable.create(local, new Path(inputFileDir.getAbsolutePath()), new TextFileReaderFactory<String>(Writables.strings(), conf));
  }

}
