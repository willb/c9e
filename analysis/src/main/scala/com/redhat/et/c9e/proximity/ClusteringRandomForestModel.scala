package com.redhat.et.c9e.analysis.proximity;

import org.apache.commons.lang3.reflect.FieldUtils

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.{Node, DecisionTreeModel, RandomForestModel}
import org.apache.spark.mllib.tree.configuration.Algo.Algo
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.linalg.{Vector => LAVec}

import org.apache.spark.rdd.RDD

private [c9e] object ClusteringNode {
  def toClusteringNode(node: Node): ClusteringNode = {
    node match {
      case n: ClusteringNode => n
      case _ => new ClusteringNode(node)
    }
  }
}

sealed abstract class PredOp
object LE extends PredOp
object GT extends PredOp
 
case class Predicate(feature: String, op: PredOp, threshold: Double) {
  override def toString = {
    val opstr = op match {
      case LE => "<="
      case GT => ">"
    }
    "Predicate(" + feature + " " + opstr + " " + threshold.toString + ")"
  }
}


// Construction is recursive on leftNode and rightNode, with a basis case where they are None:
private [c9e] class ClusteringNode(node: Node) 
    extends Node(node.id,
                 node.predict,
                 node.impurity,
                 node.isLeaf,
                 node.split,
                 node.leftNode.map(ClusteringNode.toClusteringNode),
                 node.rightNode.map(ClusteringNode.toClusteringNode),
                 node.stats) {

  def flatten: Seq[Node] = {
    if (isLeaf) List(this) else List(this) ++ leftNode.get.asInstanceOf[ClusteringNode].flatten ++ rightNode.get.asInstanceOf[ClusteringNode].flatten
  }

  private def rulesImpl(
      names: PartialFunction[Int, String],
      pstack: List[Predicate],
      rmap: mutable.Map[Double, ArrayBuffer[Seq[Predicate]]]) {
    if (isLeaf) {
      val cat = this.predict.predict
      if (!rmap.contains(cat)) rmap += (cat -> ArrayBuffer.empty[Seq[Predicate]])
      rmap(cat) += pstack.reverse
    } else {
      val t = this.split.get.threshold
      val f = this.split.get.feature
      val fname = names.applyOrElse(f, (x:Int) => "f" + x.toString)
      leftNode.get.asInstanceOf[ClusteringNode].rulesImpl(names, Predicate(fname, LE, t)::pstack, rmap)
      rightNode.get.asInstanceOf[ClusteringNode].rulesImpl(names, Predicate(fname, GT, t)::pstack, rmap)
    }
  }

  def rules(names: PartialFunction[Int, String]): Map[Double, Seq[Seq[Predicate]]] = {
    val rmap = mutable.Map.empty[Double, ArrayBuffer[Seq[Predicate]]]
    rulesImpl(names, List.empty[Predicate], rmap)
    rmap.toMap
  }

  def predictLeafId(features: LAVec): Int = {
    if (isLeaf) {
      id
    } else {
      val ln = leftNode.get.asInstanceOf[ClusteringNode]
      val rn = rightNode.get.asInstanceOf[ClusteringNode]
      if (split.get.featureType == Continuous) {
        if (features(split.get.feature) <= split.get.threshold) {
          ln.predictLeafId(features)
        } else {
          rn.predictLeafId(features)
        }
      } else {
        if (split.get.categories.contains(features(split.get.feature))) {
          ln.predictLeafId(features)
        } else {
          rn.predictLeafId(features)
        }
      }
    }
  }
}

class ClusteringTreeModel(root: Node, algo: Algo)
    extends DecisionTreeModel(ClusteringNode.toClusteringNode(root), algo) {

  def predictLeafId(features: LAVec): Int = {
    this.topNode.asInstanceOf[ClusteringNode].predictLeafId(features)
  }

  def flatten: Seq[Node] = this.topNode.asInstanceOf[ClusteringNode].flatten

  def rules(names: Map[Int, String]): Map[Double, Seq[Seq[Predicate]]] =
    this.topNode.asInstanceOf[ClusteringNode].rules(names)
}


class ClusteringRandomForestModel(model: RandomForestModel) extends Serializable {
  lazy val dtm: Array[ClusteringTreeModel] = {
    val m = FieldUtils.readField(model, "trees", true).asInstanceOf[Array[DecisionTreeModel]]
    m.map { x =>
      x match {
        case m: ClusteringTreeModel => m
        case _ => new ClusteringTreeModel(x.topNode, x.algo)
      }
    }
  }

  def rfModel: RandomForestModel = model

  def dtModels: Array[ClusteringTreeModel] = dtm

  def predictLeafIds(features: LAVec): Vector[Int] = dtm.toVector.map { _.predictLeafId(features) }

  def predictLeafIds(data: RDD[LAVec]): RDD[Vector[Int]] = data.map { this.predictLeafIds(_) }
}


object ClusteringRandomForestModel {
  implicit def toRandomForestModel(self: ClusteringRandomForestModel): RandomForestModel = {
    new RandomForestModel(self.rfModel.algo, 
                          self.dtModels.map { _.asInstanceOf[DecisionTreeModel] })
  }

  implicit def toClusteringRandomForestModel(self: RandomForestModel): ClusteringRandomForestModel = {
    new ClusteringRandomForestModel(self)
  }
}

