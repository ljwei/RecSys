package bbg.tsinghua.algrithms.cf


import bbg.tsinghua.evaluation.Evaluation._
import bbg.tsinghua.util.{Spark, HdfsDao}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by jwli on 7/25/15.
 */
class ItemCF(topNum:Int, isRate:Boolean) extends Serializable{

  private def inputFormat(line:String):(String, (String, Double)) = {
    val words = line.split(",")
    if (isRate) {
      return (words(0), (words(1), words(2).toDouble))
    } else {
      return (words(0), (words(1), 1.0))
    }
  }

  private def calCooccurrence(line:(String, Iterable[(String, Double)])):List[((String, String), Double)] = {
    var output = new ListBuffer[((String, String), Double)]
    for (itOuter <- line._2) {
      for (itInner <- line._2) {
        output += (((itOuter._1, itInner._1), 1.0))
      }
    }
    return output.toList
  }

  private def multiply(lines:(String, Iterable[(String, String, Double)])):List[(String, (String, Double))] = {
    val mapU = new mutable.HashMap[String, Double]
    val mapI = new mutable.HashMap[String, Double]
    for (line <- lines._2) {
      if (line._1.equals("U")) {
        mapU(line._2) = line._3
      } else if (line._1.equals("I")) {
        mapI(line._2) = line._3
      }
    }

    val output = new ListBuffer[(String, (String, Double))]
    val itU = mapU.keySet.iterator
    while (itU.hasNext) {
      val mapU_K = itU.next() // itemID

      val num = mapU(mapU_K)
      val itI = mapI.keySet.iterator
      while (itI.hasNext) {
        val mapI_K = itI.next() // userID
        val pref = mapI(mapI_K)
        val result = num * pref // 矩阵乘法相乘计算
        output += ((mapU_K, (mapI_K, result)))
      }
    }
    return output.toList
  }

  private def add(lines:(String, Iterable[(String, Double)])):List[((String, String), Double)] = {
    val map = new mutable.HashMap[String, Double]

    for (line <- lines._2) {
      val itemID = line._1
      val score:Double = line._2

      if (map.contains(itemID)) {
        map.put(itemID, map(itemID) + score) // 矩阵乘法求和计算
      } else {
        map.put(itemID, score)
      }
    }

    val output = new ListBuffer[((String, String), Double)]
    val iter = map.keySet.iterator
    while (iter.hasNext) {
      val itemID = iter.next()
      val score = map(itemID)
      output += (((lines._1, itemID), score))
    }
    return output.toList
  }

  private def getTopN(lines:(String, Iterable[(String, Double)])):(String, List[String]) = {
    val map = new mutable.HashMap[String, Double]
    for (line <- lines._2) {
      map(line._1) = line._2
    }
    val sortedList = map.toList.sortBy(-_._2)
    var count = 0
    val output = new ListBuffer[String]
    for (l <- sortedList) {
      println(l)
      if (count < topNum) {
        output += l._1
        count += 1
      }
    }
    return (lines._1, output.toList)
  }

  def run(inputPath:String, outputPath:String, trainTestRatio:Int, sampleRation:Double) {
    val inputData = Spark.sc.textFile(HdfsDao.HDFS + inputPath).sample(false, sampleRation)


    val inputFormatedData = inputData.map(inputFormat)

    val dataSplits = inputFormatedData.randomSplit(Array(trainTestRatio, 1))
    val trainData = dataSplits(0)
    val testData = dataSplits(1)

    trainData.cache()
    testData.cache()

    val cooccurrenceMatrix = trainData
      .groupByKey()
      .flatMap(calCooccurrence)
      .reduceByKey(_ + _)
      .map(line => (line._1._1, ("I", line._1._2, line._2)))

    val userRateMatrix = trainData
      .map(line => (line._2._1, ("U", line._1, line._2._2)))

    val result = userRateMatrix
      .union(cooccurrenceMatrix)
      .groupByKey()
      .flatMap(multiply)
      .groupByKey()
      .flatMap(add)

    val userHistory = trainData
      .map(line => ((line._1, line._2._1), Double.NegativeInfinity))

    val recommendation = result
      .subtractByKey(userHistory)
      .map(line => (line._1._1, (line._1._2, line._2)))
      .groupByKey()
      .map(getTopN)


//    recommendation
//      .cache()
//      .sortByKey()
//      .collect()
//      .foreach(println)
//    println("Recommendation result:")

    recommendation.saveAsTextFile(HdfsDao.HDFS + outputPath)

    val truth = testData
      .map(line => (line._1, line._2._1))
      .groupByKey()

    truth
      .cache()
      .sortByKey()
      .collect()
      .foreach(println)
    println("Truth:")

    val evaluationPerUser = recommendation
      .fullOuterJoin(truth)
      .mapValues(line => f1(line._1, line._2))

    evaluationPerUser.cache()

//    evaluationPerUser
//      .sortByKey()
//      .collect()
//      .foreach(println)
//    println(":f1 value of per user")

    val userCount = evaluationPerUser.count()

    val evaluationValue = evaluationPerUser.map(_._2).reduce(_ + _)/userCount
    println("f1: " + evaluationValue)
  }
}
