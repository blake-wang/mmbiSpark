package com.mm.bi.utils

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}

/**
  * Created by ElonLo on 1/9/2018.
  */
class TempFileInputFilterUtil extends PathFilter{
  override def accept(path: Path): Boolean = {
    try {
      val fs = path.getFileSystem(new Configuration)
      if (fs.isDirectory(path)) return true
      return !path.getName.endsWith("tmp")
    } catch {
      case e: IOException => e.printStackTrace()
    }
    false
  }
}
