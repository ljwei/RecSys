package bbg.tsinghua.util
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by jwli on 7/26/15.
 */
object Spark {
  val sc = new SparkContext(new SparkConf().setAppName("Recommendation System").setMaster("spark://166.111.82.56:7077")
    .setJars(List("/Users/jwli/Documents/spark_workspace/RecSys/out/artifacts/RecSys_jar/RecSys.jar")))
}
