package com.xupt.demo01

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocalFileSystem, Path}

/**
  * @author Wnlife 
  */
object ReadFile {
  def main(args: Array[String]): Unit = {
    val localFileSystem: LocalFileSystem = FileSystem.getLocal(new Configuration())
    val fileStatuses: Array[FileStatus] = localFileSystem.listStatus(new Path("datas"))

    for (f<-fileStatuses){
      println(f.getPath.toString)
    }


  }
}
