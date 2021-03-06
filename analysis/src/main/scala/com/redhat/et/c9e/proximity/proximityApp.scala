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
 * limitations under the License.
 */

package com.redhat.et.c9e.analysis.proximity;

import com.redhat.et.silex.app.AppCommon

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._


import org.apache.log4j._

object ProximityApp extends AppCommon with java.io.Serializable {
  import org.apache.log4j._

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  addConfig { conf =>
    conf
     .set("es.nodes", sys.env.getOrElse("ES_NODES", "localhost"))
     .set("es.nodes.discovery", "false")
  }

  def appName = "Machine role analysis"

  def disableLogging() {
    import collection.JavaConversions._

    val loggers = LogManager.getRootLogger() :: (LogManager.getCurrentLoggers().toList)
    loggers.foreach { _.asInstanceOf[Logger].setLevel(Level.OFF) }
  }

  def appMain(args: Array[String]) {

    disableLogging()

    val sourceDoc = sys.env.getOrElse("ES_DOC", "vos.sosreport-201504/installed-rpms")

    val sourceFrame = sys.env.get("ES_TEST_DATA") match {
      case Some(file) => sqlContext.parquetFile(file)
      case None => sqlContext.esDF(sourceDoc)
    }

    val featNames = FrontEnd.rpmMap(sourceFrame).map { case (k, v) => (v, k) }
    val nodeNames = FrontEnd.allNodes(sourceFrame).distinct.cache
    val predictTrainData = FrontEnd.labeledPoints(sourceFrame)
    val rawFeatures = FrontEnd.genRawFeatures(sourceFrame)
    
    val tmu = new TreeModelUtils(featNames, predictTrainData, nodeNames, rawFeatures)

    tmu.cluster()
    tmu.predict()
  }
}
