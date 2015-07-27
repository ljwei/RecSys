package bbg.tsinghua

import bbg.tsinghua.util._
import bbg.tsinghua.algrithms.cf._

/**
 * Created by jwli on 7/25/15.
 */
object Main {

  def main (args: Array[String]) {
    val itemCF = new ItemCF(1, false)
    val inputPath = "/user/hadoop/mydata/cf_feature.txt"
    val outputPath = "/user/hadoop/result"
    if (HdfsDao.isExist(outputPath))
      HdfsDao.rmr(outputPath)
    itemCF.run(inputPath, outputPath, 5, 0.2)
    Spark.sc.stop()
  }
}
