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

import com.redhat.et.silex.app.AppCommon

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

object ProximityApp extends AppCommon with java.io.Serializable {
  addConfig { conf =>
    conf
     .set("es.nodes", sys.env.getOrElse("ES_NODES", "localhost"))
     .set("es.nodes.discovery", "false")
  }

  def appName = "Machine role analysis"

  def appMain(args: Array[String]) {
    val sourceFrame = sys.env.get("ES_TEST_DATA") match {
      case Some(file) => sqlContext.parquetFile(file)
      case None => sqlContext.esDF("vos.sosreport-latest/installed-rpms")
    }

    FrontEnd.rpmMap(sourceFrame).foreach {
      case (k, v) => Console.println(s"$k --> $v")
    }

    val featNames = FrontEnd.rpmMap(sourceFrame).map { case (k, v) => (v, k) }
    val nodeNames = context.parallelize(FrontEnd.nodes(sourceFrame))
    val predictTrainData = FrontEnd.labeledPoints(sourceFrame)
    val rawFeatures = FrontEnd.genRawFeatures(sourceFrame)
    
    val tmu = new TreeModelUtils(featNames, predictTrainData, nodeNames, rawFeatures)
    tmu.cluster()
    tmu.predict()
  }
}
