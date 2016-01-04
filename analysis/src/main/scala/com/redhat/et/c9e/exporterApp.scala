/*
 * This file is part of the "consigliere" toolkit for sosreport
 * and SAR data analytics and visualization.
 *
 * Copyright (c) 2016 Red Hat, Inc.
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

package com.redhat.et.c9e;

import com.redhat.et.silex.app.AppCommon

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

import scala.util.Try

import org.apache.log4j._

object ExporterApp extends AppCommon with java.io.Serializable {
  import org.apache.log4j._

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  addConfig { conf =>
    conf
     .set("es.nodes", sys.env.getOrElse("C9E_ES_NODES", "localhost"))
     .set("es.nodes.discovery", "false")
  }

  def appName = "ES data extractor"

  def appMain(args: Array[String]) {
    val dates = (11 to 12).flatMap { month => (1 to 31).map { day => (month, day) } }.collect { case (m,d) if m == 12 || d > 8 => "2015.%02d.%02d".format(m, d) }
    dates.foreach { date => Try(sqlContext.esDF(s"logstash-$date/rsyslog").repartition(64).saveAsParquetFile(s"logstash-${date.replace(".", "")}-rsyslog.parquet")) }
  }
}
