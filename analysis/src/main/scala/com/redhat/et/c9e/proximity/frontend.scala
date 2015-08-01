/*
 * This file is part of the "consigliere" toolkit for sosreport
 * and SAR data analytics and visualization.
 *
 * Copyright (c) 2015 Red Hat, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.c
 */

package com.redhat.et.c9e.analysis.proximity;

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors=>V, Vector=>VEC, DenseVector}

import com.redhat.et.silex.app.AppCommon

trait ProximityFE[A <: AppCommon] extends ClusterLabels {
  self: A =>
  
  val sqlc = sqlContext
  
  import com.redhat.et.silex.util.RegexImplicits._

  def nameFromRpm(rpm: String): Either[String, String] = rpm match {
    case r"(.*)$name[-][^-]*[-].*" => Right(name)
    case _ => Left(rpm)
  }
  
  def listRpms(df: DataFrame): RDD[(String, Array[String])] = {
    import sqlc.implicits._
    
    val projection = df.select($"_metadata.nodename", $"installed-rpms")
    projection.map { 
      case Row(host: String, rawRpms: String) => {
        val rpms = rawRpms.split("\n").map(_.split(" ")(0)).map(nameFromRpm(_)).collect { case Right(rpm) => rpm }
        (host, rpms)
      }
    }
  }
  
  def featurize(df: DataFrame, rpmsForNodes: RDD[(String, Array[String])]): RDD[(String, VEC)] = {
    import sqlc.implicits._
    val rpmIds = context.broadcast(rpmMap(df))
    
    rpmsForNodes.map { 
      case (host, rpms) =>
	(host, featuresForNode(host, rpms, rpmIds.value))
    }
  }
  

  def featuresForNode(node: String, rpms: Array[String], rpmIds: Map[String, Int]) = {
    // XXX: this is shamefully lazy
    val sparse = V.sparse(rpmIds.size, rpms.map(rpmIds(_)), Array.fill(rpms.size)(1.0))
    V.dense(sparse.toArray)
  }

  def rpmMap(df: DataFrame): Map[String, Int] = {
    import sqlc.implicits._
    val projection = df.select($"_metadata.nodename", $"installed-rpms")
    Map(projection
         .flatMap { 
           case Row(h: String, rpms: String) => rpms.split("\n").map {_.split(" ")(0)} 
          }
         .distinct
         .sortBy{x:String => x}
         .collect
         .zipWithIndex : _*)
  }
  
  def nodes(df: DataFrame): Seq[String] = {
    import sqlc.implicits._
    df
      .select($"_metadata.nodename")
      .map { case Row(node: String) => node.toString }
      .collect()
      .collect { case s: String if labels.isDefinedAt(s) => s } 
  }

  def labeledNodes(df: DataFrame): Map[String, Int] = {
    Map(nodes(df).map { n => (n, labels(n)) } : _*)
  }
}
	    

trait ClusterLabels {
  // these are cluster labels for supervised learning

  val labels = Map[String, Int](
    ("brewweb.devel.redhat.com" -> 0),
    ("errata.devel.redhat.com" -> 0),
    ("bugweb-01.app.bz.hst.phx2.redhat.com" -> 1),
    ("bugweb-02.app.bz.hst.phx2.redhat.com" -> 1),
    ("bugweb-spare.app.bz.hst.phx2.redhat.com" -> 1),
    ("bzdb01.db.bz.hst.phx2.redhat.com" -> 2),
    ("bzdb02.db.bz.hst.phx2.redhat.com" -> 2),
    ("bzdb03.db.bz.hst.phx2.redhat.com" -> 2),
    ("bzdb04.db.bz.hst.phx2.redhat.com" -> 2),
    ("bzdb05.db.bz.hst.phx2.redhat.com" -> 2),
    ("bzweb01.app.bz.hst.phx2.redhat.com" -> 3),
    ("bzweb02.app.bz.hst.phx2.redhat.com" -> 3),
    ("ceehadoop1.gsslab.rdu2.redhat.com" -> 4),
    ("ceehadoop2.gsslab.rdu2.redhat.com" -> 4),
    ("ceehadoop3.gsslab.rdu2.redhat.com" -> 4),
    ("ceehadoop4.gsslab.rdu2.redhat.com" -> 4),
    ("confluence-01.app.eng.rdu2.redhat.com" -> 5),
    ("jira-01.app.eng.rdu2.redhat.com" -> 5),
    ("virt-atlassian.eng.rdu2.redhat.com" -> 5),
    ("control-01.infra.prod.eng.rdu2.redhat.com" -> 6),
    ("control-01.infra.stage.eng.rdu2.redhat.com" -> 6),
    ("control-02.infra.prod.eng.rdu2.redhat.com" -> 6),
    ("git.app.eng.bos.redhat.com" -> 7),
    ("git.engineering.redhat.com" -> 7),
    ("svn.app.eng.rdu2.redhat.com" -> 7),
    ("svn2.app.eng.rdu2.redhat.com" -> 7),
    ("ex-c9-node11.prod.rhcloud.com" -> 8),
    ("ex-c9-node13.prod.rhcloud.com" -> 8),
    ("ex-lrg-node1.prod.rhcloud.com" -> 8),
    ("ex-lrg-node2.prod.rhcloud.com" -> 8),
    ("ex-med-node1.prod.rhcloud.com" -> 8),
    ("ex-med-node2.prod.rhcloud.com" -> 8),
    ("ex-std-node100.prod.rhcloud.com" -> 8),
    ("ex-std-node200.prod.rhcloud.com" -> 8),
    ("ex-std-node300.prod.rhcloud.com" -> 8),
    ("gprfc001.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc002.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc003.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc004.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc005.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc006.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc007.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc008.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc009.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc010.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc011.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc012.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc013.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc014.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc015.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc016.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc042.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc043.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc044.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfc045.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs009.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs010.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs011.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs012.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs013.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs014.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs015.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs016.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs025.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs026.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs027.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs028.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs029.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs030.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs031.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gprfs032.sbu.lab.eng.bos.redhat.com" -> 9),
    ("gss-servicejava01.web.prod.ext.phx2.redhat.com" -> 10),
    ("gss-servicejava02.web.prod.ext.phx2.redhat.com" -> 10),
    ("gss-servicejava03.web.prod.ext.phx2.redhat.com" -> 10),
    ("gss-servicejava04.web.prod.ext.phx2.redhat.com" -> 10),
    ("gss-webjava01.web.prod.ext.phx2.redhat.com" -> 11),
    ("gss-webjava02.web.prod.ext.phx2.redhat.com" -> 11),
    ("gss-webjava03.web.prod.ext.phx2.redhat.com" -> 11),
    ("gss-webjava04.web.prod.ext.phx2.redhat.com" -> 11),
    ("kcs01.web.prod.ext.phx2.redhat.com" -> 12),
    ("kcs02.web.prod.ext.phx2.redhat.com" -> 12),
    ("kcs03.web.prod.ext.phx2.redhat.com" -> 12),
    ("openstack-cinder-storage-01.qeos.lab.eng.rdu2.redhat.com" -> 13),
    ("openstack-cinder-storage-02.qeos.lab.eng.rdu2.redhat.com" -> 13),
    ("openstack-compute-01.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-02.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-03.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-04.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-05.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-06.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-07.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-08.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-09.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-10.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-11.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-12.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-13.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-14.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-15.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-16.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-17.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-18.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-19.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-20.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-21.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-22.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-23.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-24.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-compute-25.qeos.lab.eng.rdu2.redhat.com" -> 14),
    ("openstack-controller-01.qeos.lab.eng.rdu2.redhat.com" -> 15),
    ("openstack-controller-02.qeos.lab.eng.rdu2.redhat.com" -> 15),
    ("openstack-glance-storage-01.qeos.lab.eng.rdu2.redhat.com" -> 16),
    ("openstack-glance-storage-02.qeos.lab.eng.rdu2.redhat.com" -> 16),
    ("openstack-network-01.qeos.lab.eng.rdu2.redhat.com" -> 17),
    ("openstack-network-02.qeos.lab.eng.rdu2.redhat.com" -> 17),
    ("perf1.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf4.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf44.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf46.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf48.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf56.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf60.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf70.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf72.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf78.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf80.perf.lab.eng.bos.redhat.com" -> 18),
    ("perf92.perf.lab.eng.bos.redhat.com" -> 18)
  )
}
