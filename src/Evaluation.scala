package bbg.tsinghua.evaluation

/**
 * Created by jwli on 7/27/15.
 */
object Evaluation {
  def precision(truth:Option[List[String]], recommendation:Option[Iterable[String]]):Double = {
    if (truth.isEmpty || recommendation.isEmpty) {
      return 0.0
    } else {
      val truthSet = truth.get.toSet
      val recommSet = recommendation.get.toSet
      return truthSet.intersect(recommSet).size.toDouble / recommSet.size.toDouble
    }
  }

  def recall(truth:Option[List[String]], recommendation:Option[Iterable[String]]):Double = {
    if (truth.isEmpty || recommendation.isEmpty) {
      return 0.0
    } else {
      val truthSet = truth.get.toSet
      val recommSet = recommendation.get.toSet
      return truthSet.intersect(recommSet).size.toDouble / truthSet.size.toDouble
    }
  }

  def f1(truth:Option[List[String]], recommendation:Option[Iterable[String]]):Double = {
    if (truth.isEmpty || recommendation.isEmpty) {
      return 0.0
    } else {
      val truthSet = truth.get.toSet
      val recommSet = recommendation.get.toSet

      val intersectSet = truthSet.intersect(recommSet)
      if (intersectSet.size == 0) {
        return 0.0
      }
      val p = intersectSet.size.toDouble / recommSet.size.toDouble
      val r = intersectSet.size.toDouble / truthSet.size.toDouble
      return 2 * p * r / (p + r)
    }
  }
}
