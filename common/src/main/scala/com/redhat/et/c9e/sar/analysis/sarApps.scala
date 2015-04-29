/*
 * This file is part of the "consigliere" toolkit for sosreport
 * and SAR data analytics and visualization.
 *
 * Copyright (c) 2014 Red Hat, Inc.
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

package com.redhat.et.c9e.sar.analysis

import com.redhat.et.c9e.common.{SarIngest, PathOperations}

import com.redhat.et.silex.app.AppCommon

object SarModeler extends AppCommon with SarCommon {
  import org.apache.spark.mllib.feature.Normalizer
  import org.apache.spark.mllib.linalg.{Vectors=>V, Vector=>VEC, DenseVector}

  case class NR(nodename: String, timestamp: Long, kind: String, v: org.apache.spark.mllib.linalg.Vector) {
    def toCSV = {
      val sb = new StringBuilder(nodename)
      sb.append(",")
      sb.append(timestamp)
      sb.append(",")
      sb.append(kind)
      sb.append(",")
      v.toArray.addString(sb, ",")
      sb.toString
    }
  }

  override def appName = "sar modeler"

  def ingest[A <: AppCommon](args: Array[String], app: A = this) = {
    new SarIngest(args, app)
  }

  def normalizedMemory[A <: AppCommon](si: SarIngest[A]) = {
    val nm = new Normalizer
    val normalizedVecs = nm.transform(si.memoryEntries.map(_.toVec))
    si.memoryEntries
     .map(me => Pair(me.nodename, me.timestamp))
     .zip(normalizedVecs)
     .map { case ((nn, ts), vecs) => NR(nn, ts.getTime, "memory", vecs) }
  }

  def makeTable[A <: AppCommon](args: Array[String], app: A) = {
    val sc = app.sqlContext
    import sc.implicits._
    
    val nm = normalizedMemory(ingest(args, app)).toDF()
    nm.registerTempTable("nm")
    nm
  }

  def makeSchemaRDD[A <: AppCommon](args: Array[String], app: A) = {
    val sc = app.sqlContext
    import sc.implicits._
    
    normalizedMemory(ingest(args, app)).toDF()
  }
  
  def loadParquetFile[A <: AppCommon](pf: String, app: A = this) = {
    val s_rdd = app.sqlContext.parquetFile(pf)
    val cc_rdd = s_rdd.map { row =>
      NR(row.getAs[String](0), row.getAs[Long](1), row.getAs[String](2), row.getAs[VEC](3))
    }
    Pair(s_rdd, cc_rdd)
  }

  def appMain(args: Array[String]) {
    val sc = this.sqlContext
    val options = parseArgs(args)
    
    import sc.implicits._
    
    val nm = makeTable(options.toBasicArgs, this).toDF()
    nm.saveAsParquetFile(options.parquetOut)
  }
}

trait SarCommon extends PathOperations {
  case class SarOptions(inputFiles: Vector[String], outputDir: String, parquetOut: String) {
    def withFile(f: String) = this.copy(inputFiles=inputFiles:+f)
    def withFiles(fs: Seq[String]) = this.copy(inputFiles=inputFiles++fs)
    def withOutputDir(d: String) = this.copy(outputDir=d)
    def withParquetOutput(d: String) = this.copy(parquetOut=d)
    def toBasicArgs = ("--outputDir" +: outputDir +: "--" +: inputFiles).toArray
  }
  
  object SarOptions {
    def default = SarOptions(Vector[String](), ".", "sar-parquet")
  }
  
  def parseArgs(args: Array[String]) = {
    def phelper(params: List[String], options: SarOptions): SarOptions = {
      params match {
        case Nil => options
        case "--output-dir" :: dir :: rest => phelper(rest, options.withOutputDir(dir))
        case "--input-dir" :: dir :: rest => phelper(rest, options.withFiles(listFilesInDir(dir)))
        case "--parquet-out" :: loc :: rest => phelper(rest, options.withParquetOutput(loc))
        case "--" :: rest => options.withFiles(rest)
        case bogusOpt if bogusOpt(0) == "-" => throw new RuntimeException(s"unrecognized option $bogusOpt")
        case file :: rest => phelper(rest, options.withFile(file))
      }
    }
    phelper(args.toList, SarOptions.default)
  }
}

