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

import com.redhat.et.c9e.common.{AppCommon, SarIngest, PathOperations}
import org.apache.spark.mllib.feature.Normalizer

object SarModeler extends AppCommon with SarCommon {
  override def appName = "sar modeler"

  def ingest(args: Array[String]) = {
    new SarIngest(args, this)
  }

  def normalizedMemory[A <: AppCommon](si: SarIngest[A]) = {
    val nm = new Normalizer
    val normalizedVecs = nm.transform(si.memoryEntries.map(_.toVec))
    si.memoryEntries.map(me => Pair(me.nodename, me.timestamp)).zip(normalizedVecs)
  }

  

  def appMain(args: Array[String]) {
    
  }
}

trait SarCommon extends PathOperations {
  case class SarOptions(inputFiles: Vector[String], outputDir: String) {
    def withFile(f: String) = this.copy(inputFiles=inputFiles:+f)
    def withFiles(fs: Seq[String]) = this.copy(inputFiles=inputFiles++fs)
    def withOutputDir(d: String) = this.copy(outputDir=d)
  }
  
  object SarOptions {
    def default = SarOptions(Vector[String](), ".")
  }
  
  def parseArgs(args: Array[String]) = {
    def phelper(params: List[String], options: SarOptions): SarOptions = {
      params match {
        case Nil => options
        case "--output-dir" :: dir :: rest => phelper(rest, options.withOutputDir(dir))
        case "--input-dir" :: dir :: rest => phelper(rest, options.withFiles(listFilesInDir(dir)))
        case "--" :: rest => options.withFiles(rest)
        case bogusOpt if bogusOpt(0) == "-" => throw new RuntimeException(s"unrecognized option $bogusOpt")
        case file :: rest => phelper(rest, options.withFile(file))
      }
    }
    phelper(args.toList, SarOptions.default)
  }
}

