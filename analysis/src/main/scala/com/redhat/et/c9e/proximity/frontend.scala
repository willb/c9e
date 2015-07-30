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

import com.redhat.et.silex.app.AppCommon

trait ProximityFrontEnd[A <: AppCommon] {
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
  
  
}