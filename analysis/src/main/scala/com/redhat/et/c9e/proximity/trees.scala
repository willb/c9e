package com.redhat.et.c9e.analysis.proximity;

import org.apache.log4j.{Logger, ConsoleAppender, Level}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{Vector => LAVec, DenseVector => LADenseVec}
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.rdd.RDD

import ClusteringRandomForestModel._
import RandomForestClustering._

trait TreeModelUtils {
  // a map from each feature index to its name
  val featNames: Map[Int, String]

  // from original implementation:
  // val raw = spark.textFile("rj_rpm_data/train.txt").map(_.split(" ").map(_.toDouble))
  // val trainData = raw.map(x => LabeledPoint(x.head, new LADenseVec(x.tail)))
  val predictTrainData: RDD[LabeledPoint]
  
  // from original implementation:
  // val nodeNames = spark.textFile("rj_rpm_data/sortnodes.txt").map { _.split(" ")(1) }
  val nodeNames: RDD[String]
  
  def predict(spark: SparkContext, numTrees: Int = 10, maxDepth: Int = 5) {
    // turn off spark logging spam in the REPL
    Logger.getRootLogger().getAppender("console").asInstanceOf[ConsoleAppender].setThreshold(Level.WARN)


    // largest class number is 19.0
    val numClasses = 20

    // all binary features:
    val categoricalFeaturesInfo = Map[Int, Int]().withDefaultValue(2)
    val maxBins = 2

    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val model = RandomForest.trainClassifier(predictTrainData,
                                             numClasses,
                                             categoricalFeaturesInfo,
                                             numTrees,
                                             featureSubsetStrategy,
                                             impurity,
                                             maxDepth,
                                             maxBins)

    for (dt <- model.dtModels.take(3)) {
      println(s"\n${dt.toDebugString}")
    }

    // compute model error rate
    val labelAndPreds = predictTrainData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val err = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / predictTrainData.count
    println("Error rate = " + err)    

    val fh = featureHist(model)
    println(s"\ntotal features used = ${fh.length}")
    val t = fh.take(20).map(x=>(x._2,featNames(x._1))).mkString(" ")
    println(s"$t")


    val rules = rfRules(model, featNames)
    for (cat <- rules.keys) {
      println(s"\n\nrules for class $cat")
      for (r <- rules(cat)) {
        val rr = r.map { x =>
          if (x.op == LE) ("not " + x.feature) else x.feature  
        }
        val ss = rr.mkString(" && ")
        println(s"    $ss")
      }
    }
  }


  def cluster(spark: SparkContext, numTrees: Int = 10, maxDepth: Int = 5, nClust: Int = 5, outlierThreshold: Double = 3.0) {
    // turn off spark logging spam in the REPL
    Logger.getRootLogger().getAppender("console").asInstanceOf[ConsoleAppender].setThreshold(Level.WARN)

    // all but the labels at the beginning
    val raw = spark.textFile("/home/eje/rj_rpm_data/train.txt").map(_.split(" ").map(_.toDouble).tail)
    val iid = iidSynthetic(raw, 250)

    val realData = raw.map(x => LabeledPoint(1.0, new LADenseVec(x.tail)))
    val iidData = iid.map(x => LabeledPoint(0.0, new LADenseVec(x.tail)))
    val trainData = realData.union(iidData)

    // clustering is binary classification problem
    val numClasses = 2

    // all binary features:
    val categoricalFeaturesInfo = Map[Int, Int]().withDefaultValue(2)
    val maxBins = 2

    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val model = RandomForest.trainClassifier(trainData,
                                             numClasses,
                                             categoricalFeaturesInfo,
                                             numTrees,
                                             featureSubsetStrategy,
                                             impurity,
                                             maxDepth,
                                             maxBins)

    for (dt <- model.dtModels.take(3)) {
      println(s"\n${dt.toDebugString}")
    }

    // compute model error rate
    val labelAndPreds = trainData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val err = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / trainData.count
    println("Error rate = " + err)    

    val fh = featureHist(model)
    println(s"\ntotal features used = ${fh.length}")
    val t = fh.take(20).map(x=>(x._2,featNames(x._1))).mkString(" ")
    println(s"$t")

    val leafIdData = model.predictLeafIds(realData.map(_.features))
    val (clusters, metric) = kMedoids(leafIdData, nClust, leafIdDist, maxIterations=10)
    println(s"clusters= $clusters")
    println(s"metric= $metric")

    val nodeLeafs = nodeNames.collect.zip(leafIdData.collect)
    val mdf = (x: Vector[Int], mv: Seq[Vector[Int]]) => {
      mv.view.zipWithIndex.map { z => (leafIdDist(x, z._1), z._2) }.min
    }
    val clusteredNodes = nodeLeafs.map { x =>
      val (dist, clust) = mdf(x._2, clusters)
      (x._1, dist, clust)
    }

    val byCluster = clusteredNodes.filter(_._2 <= outlierThreshold).groupBy((x: (String, Double, Int)) => x._3)
    for (k <- byCluster.keys.toSeq.sorted) {
      println(s"\n\ncluster ($k)")
      for (x <- byCluster(k).sortBy(_._2)) {
        println(s"    clust= ${x._3}  dist= ${x._2}  node= ${x._1}")
      }
    }
    println(s"\n\nOutliers")
    for (x <- clusteredNodes.filter(_._2 > outlierThreshold).sortBy(z=>(z._3, z._2))) {
      println(s"    clust= ${x._3}  dist= ${x._2}  node= ${x._1}")
    }
  }
}
