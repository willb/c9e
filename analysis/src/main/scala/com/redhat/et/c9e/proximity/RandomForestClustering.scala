package com.redhat.et.c9e.analysis.proximity;

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD

object RandomForestClustering {

  def featureHist(model: ClusteringRandomForestModel): Seq[(Int,Int)] = {
    val raw = model.dtModels.flatMap(_.flatten.filter(!_.isLeaf)).map(_.split.get.feature)
    val hist = mutable.Map.empty[Int, Int]
    raw.foldLeft(hist)((h, x) => {
      val n = h.getOrElse(x, 0)
      h += (x -> (1 + n))
      h
    })
    hist.toSeq.sortBy(_._2).reverse
  }

  def rfRules(model: ClusteringRandomForestModel, names: Map[Int, String]): Map[Double, Seq[Seq[Predicate]]] = {
    val dtr = model.dtModels.map(_.rules(names))
    val rfr = mutable.Map.empty[Double, Seq[Seq[Predicate]]]
    dtr.foldLeft(rfr)((r, x) => {
      for (k <- x.keys) {
        val s = r.getOrElse(k, Seq.empty[Seq[Predicate]])
        r += (k -> (s ++ x(k)))
      }
      r
    })
    rfr.toMap
  }

  def iidSynthetic[T :ClassTag](
      data: RDD[Array[T]],
      nSynth: Int,
      sampleSize: Int = 10000): RDD[Array[T]] = {
    val n = data.count
    val ss = math.min(sampleSize, n).toInt
    val fraction = math.min(1.0, ss.toDouble / n.toDouble)
    val parts = ArrayBuffer.empty[RDD[Array[T]]]

    var nn = 0
    while (nn < nSynth) {
      val sample = data.sample(false, fraction).collect
      val l = sample.length
      val m = sample.head.length
      val transposed = Array.fill(m)(ArrayBuffer.empty[T])
      sample.foldLeft(transposed)((t, x) => {
        for (xx <- t.zip(x)) { xx._1 += xx._2 }
        t
      })
      val permuted = transposed.map(Random.shuffle(_))
      val iid = Array.fill(l)(ArrayBuffer.empty[T])
      permuted.foldLeft(iid)((i, x) => {
        for (xx <- i.zip(x)) { xx._1 += xx._2 }
        i
      })
      parts += data.sparkContext.parallelize(iid.map(_.toArray))
      nn += l
    }

    new org.apache.spark.rdd.UnionRDD(data.sparkContext, parts)
  }

  private def kMedoidsRefine[T, U >: T](
      data: Seq[T],
      metric: (U, U) => Double,
      initial: Seq[T],
      initialCost: Double,
      cost: (Seq[T], Seq[T]) => Double,
      medoidCost: (T, Seq[T]) => Double,
      maxIterations: Int): (Seq[T], Double, Int, Boolean) = {

    val k = initial.length
    val medoidIdx = (e: T, mv: Seq[T]) => mv.view.map(metric(e, _)).zipWithIndex.min._2
    val medoid = (data: Seq[T]) => data.view.minBy(medoidCost(_, data))

    var current = initial
    var currentCost = initialCost
    var converged = false

    var itr = 0
    var halt = itr >= maxIterations
    while (!halt) {
      val next = data.groupBy(medoidIdx(_, current)).toVector.sortBy(_._1).map(_._2).map(medoid)
      val nextCost = cost(next, data)

      if (nextCost >= currentCost) {
        converged = true
        halt = true
      } else {
        current = next
        currentCost = nextCost
      }

      itr += 1
      if (itr >= maxIterations) halt = true
    }

    (current, currentCost, itr, converged)
  }
        

  def kMedoids[T :ClassTag, U >: T :ClassTag](
      data: RDD[T],
      k: Int,
      metric: (U,U) => Double,
      sampleSize: Int = 10000,
      maxIterations: Int = 10,
      resampleInterval: Int = 3
    ): (Seq[T], Double) = {

    val n = data.count
    require(k > 0)
    require(n >= k)

    val ss = math.min(sampleSize, n).toInt
    val fraction = math.min(1.0, ss.toDouble / n.toDouble)
    var sample: Array[T] = data.sample(false, fraction).collect

    // initialize medoids to a set of (k) random and unique elements
    var medoids: Seq[T] = Random.shuffle(sample.toSeq.distinct).take(k)
    require(medoids.length >= k)

    val minDist = (e: T, mv: Seq[T]) => mv.view.map(metric(e, _)).min
    val cost = (mv: Seq[T], data: Seq[T]) => data.view.map(minDist(_, mv)).sum
    val medoidCost = (e: T, data: Seq[T]) => data.view.map(metric(e, _)).sum

    var itr = 1
    var halt = itr > maxIterations
    var lastCost = cost(medoids, sample)
    while (!halt) {
      println(s"\n\nitr= $itr")
      // update the sample periodically
      if (fraction < 1.0  &&  itr > 1  &&  (itr % resampleInterval) == 0) {
        sample = data.sample(false, fraction).collect
      }

      val (nxt, nxtCost, _, _) = 
      kMedoidsRefine(
        sample,
        metric,
        medoids,
        lastCost,
        cost,
        medoidCost,
        1)

      // todo: test some function of metric values over time as an optional halting condition
      // when improvement stops
      println(s"last= $lastCost  new= $nxtCost")
      lastCost = nxtCost
      medoids = nxt

      itr += 1
      if (itr > maxIterations) halt = true
    }

    // return most recent cluster medoids
    (medoids, lastCost)
  }


  // Cumulative Residual Entropy
  def CRE(data: Seq[Double]): Double = {
    val n = data.length
    if (n < 1) { return 0.0 }
    val sorted = data.sorted
    val hist = ArrayBuffer.empty[(Double, Double)]
    val r = sorted.tail.foldLeft((hist, sorted.head, 1.0))((t, x) => {
      val (h, prev, c) = t
      if (x == prev) (h, prev, c + 1.0) else {
        h += (prev -> c)
        (h, x, 1.0)
      }
    })
    hist += (r._2 -> r._3)
    val dn = n.toDouble
    var cre = 0.0
    var cr = 1.0
    for (j <- 0 until hist.length - 1) {
      val (xj, fj) = hist(j)
      val (xj1, _) = hist(j+1)
      cr -= fj / dn
      cre -= (xj1 - xj)*cr*math.log(cr)
    }
    cre
  }


  def xMedoids[T :ClassTag, U >: T :ClassTag](
      data: RDD[T],
      metric: (U,U) => Double,
      sampleSize: Int = 10000,
      maxIterations: Int = 25,
      resampleInterval: Int = 3
    ): (Seq[T], Double) = {

    val n = data.count
    val ss = math.min(sampleSize, n).toInt
    val fraction = math.min(1.0, ss.toDouble / n.toDouble)
    var sample: Array[T] = data.sample(false, fraction).collect

    val minDist = (e: T, mv: Seq[T]) => mv.view.map(metric(e, _)).min
    val minIdx = (e: T, mv: Seq[T]) => mv.view.map(metric(e, _)).zipWithIndex.min._2
//    val cost = (mv: Seq[T], data: Seq[T]) => CRE(data.map(minDist(_, mv)))
//    val medoidCost = (e: T, data: Seq[T]) => CRE(data.map(metric(e, _)))
    val creCost = (mv: Seq[T], data: Seq[T]) => CRE(data.map(minDist(_, mv)))
    val cost = (mv: Seq[T], data: Seq[T]) => data.map(minDist(_, mv)).sum
    val medoidCost = (e: T, data: Seq[T]) => data.map(metric(e, _)).sum
    val medoid = (data: Seq[T]) => data.view.minBy(medoidCost(_, data))
    val antipodes = (e: T, data: Seq[T]) => {
      val a1 = data.view.maxBy(metric(e, _))
      val a2 = data.view.maxBy(metric(a1, _))
      Seq(a1, a2)
    }

    // initial model is single medoid of entire sample
    var current = Seq(medoid(sample))
    var currentCost = cost(current, sample)
    var currentData = sample.groupBy(minIdx(_, current))

    val initCost = creCost(current, sample)
    println(s"initCost= $initCost")

    var itr = 1
    var halt = itr > maxIterations
    while (!halt) {
      println(s"\n\nitr= $itr")

      val candidates = for (
        j <- 0 until current.length;
        cdj = currentData(j);
        cdju = cdj.distinct;
        if (cdju.length >= 2);
        (cL, t) = current.splitAt(j);
        (c, cR) = (t.head, t.tail)
      ) yield {
        val cmp = antipodes(c, cdju)
        val (rmp, _, _, _) = kMedoidsRefine(
          currentData(j),
          metric,
          cmp,
          cost(cmp, cdj),
          cost,
          medoidCost,
          3)
        val cmm = cL ++ rmp ++ cR
        println(s"    ${cL.length} ++ ${rmp.length} ++ ${cR.length} == ${cmm.length}")
        val (rmm, _, _, _) = kMedoidsRefine(
          sample,
          metric,
          cmm,
          cost(cmm, sample),
          cost,
          medoidCost,
          5)

        val rmmData = sample.groupBy(minIdx(_, rmm))
        val rmmCost = creCost(rmm, sample)
        val dCost = initCost - rmmCost
//        val penalty = (rmm.length - 1) * math.log(n)
        val penalty = rmmData.values.map { cx =>
          val p = cx.length.toDouble / sample.length.toDouble
          - p * math.log(p)
        }.sum
        val delta = dCost - penalty
        val gain = dCost / penalty
        
        println(s"j= $j  n= ${rmm.length}  cost= $rmmCost  dCost= $dCost  penalty= $penalty  delta= $delta  gain= $gain")

        (rmmCost, rmm, rmmData)
      }

      val (nextCost, next, nextData) = candidates.minBy(_._1)
      println(s"next= ${next.length}  cost= $nextCost")

      current = next
      currentCost = nextCost
      currentData = nextData

      itr += 1
      if (itr > maxIterations) halt = true
    }

    // return most recent cluster medoids
    (current, currentCost)
  }


  def leafIdDist(a: Vector[Int], b: Vector[Int]): Double = a.zip(b).count(e => (e._1 != e._2))

}
