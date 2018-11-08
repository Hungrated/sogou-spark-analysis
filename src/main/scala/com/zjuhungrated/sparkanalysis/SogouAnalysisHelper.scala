package com.zjuhungrated.sparkanalysis

import java.io.File

class SogouAnalysisHelper {

  /**
    * 删除一个文件夹及其子目录（文件）若该目录存在
    *
    * @param dir 文件或文件夹路径
    */
  def deletDirIfExists(dir: String): Unit = {
    val f = new File(dir)
    if (f.exists()) {
      deleteDir(f)
    } else {
      println("dir doesn't exist. skipped.")
    }
  }

  /**
    * 删除一个文件夹及其子目录（文件）
    *
    * @param dir 文件或文件夹路径
    */
  def deleteDir(dir: File): Unit = {
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
        println("deleted file " + f.getAbsolutePath)
      }
    })
    dir.delete()
    println("deleted dir " + dir.getAbsolutePath)
  }

}
