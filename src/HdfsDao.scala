package bbg.tsinghua.util

import java.io.ByteArrayOutputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils

/**
 * Created by jwli on 7/25/15.
 */

object HdfsDao {
  val HDFS = "hdfs://166.111.82.56:9000"
  private val conf = new Configuration()

  def ls(folder:String) = {
    val path = new Path(folder)
    val fs = FileSystem.get(URI.create(HDFS), conf)
    val list = fs.listStatus(path)
    System.out.println("ls: " + folder)
    System.out.println("==========================================================")
    for( p <- list) {
      println(p)
    }
    System.out.println("==========================================================")
    fs.close()
  }

  def mkdirs(folder:String) = {
    val path = new Path(folder)
    val fs = FileSystem.get(URI.create(HDFS), conf)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
      System.out.println("Create: " + folder)
    }
    fs.close()
  }

  def rmr(folder:String) = {
    val path = new Path(folder)
    val fs = FileSystem.get(URI.create(HDFS), conf)
    fs.deleteOnExit(path)
    System.out.println("Delete: " + folder)
    fs.close()
  }

  def rename(src:String, dst:String) = {
    val name1 = new Path(src)
    val name2 = new Path(dst)
    val fs = FileSystem.get(URI.create(HDFS), conf)
    fs.rename(name1, name2)
    System.out.println("Rename: from " + src + " to " + dst)
    fs.close()
  }

  def createFile(file:String, content:String) = {
    val fs = FileSystem.get(URI.create(HDFS), conf)
    val buff = content.getBytes()
    var os:FSDataOutputStream = null
    try {
      os = fs.create(new Path(file))
      os.write(buff, 0, buff.length)
      System.out.println("Create: " + file)
    } finally {
      if (os != null)
        os.close()
    }
    fs.close()
  }

  def put(local:String , remote:String) = {
    val fs = FileSystem.get(URI.create(HDFS), conf)
    fs.copyFromLocalFile(new Path(local), new Path(remote))
    System.out.println("put: from " + local + " to " + remote)
    fs.close()
  }

  def get(remote:String , local:String) = {
    val path = new Path(remote)
    val fs = FileSystem.get(URI.create(HDFS), conf)
    fs.copyToLocalFile(path, new Path(local))
    System.out.println("get: from " + remote + " to " + local)
    fs.close()
  }

  def cat(remoteFile:String ):String = {
    val path = new Path(remoteFile)
    val fs = FileSystem.get(URI.create(HDFS), conf)
    var fsdis:FSDataInputStream = null
    System.out.println("cat: " + remoteFile)

    val baos = new ByteArrayOutputStream()
    var str:String = null

    try {
      fsdis = fs.open(path)
      IOUtils.copyBytes(fsdis, baos, 4096, false)
      str = baos.toString()
    } finally {
      IOUtils.closeStream(fsdis)
      fs.close()
    }
    System.out.println(str)
    return str
  }

  def isExist(file:String):Boolean = {
    val path = new Path(file)
    val fs = FileSystem.get(URI.create(HDFS), conf)
    val bool = fs.exists(path)
    System.out.println("isExist: " + file + "? " + bool.toString)
    return bool
  }

  def main(args: Array[String]) {

    HdfsDao.isExist("/user/hadoop/test/1")
    HdfsDao.mkdirs("/user/hadoop/test")
    HdfsDao.mkdirs("/user/hadoop/test1")
    HdfsDao.rmr("/user/hadoop/result")
    HdfsDao.rename("/user/hadoop/test1", "/user/hadoop/test2")
    HdfsDao.createFile("/user/hadoop/test/1", "hello world!")
    HdfsDao.put("./test.txt", "/user/hadoop/test")
    HdfsDao.get("/user/hadoop/test/1", "./test1.txt")
    HdfsDao.cat("/user/hadoop/test/1")
  }
}
