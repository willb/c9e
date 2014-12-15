/*
 * This file is part of the "consigliere" toolkit for sosreport
 * data analytics and visualization.
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
 * limitations under the License.
 */

package com.redhat.et.consigliere.common;

import org.json4s._
import org.json4s.jackson.JsonMethods._

trait CleaningHelpers {
  def splitValue(sls: String, delim: String = " ") = {
    JArray(sls.split(delim).map(JString(_)).toList)
  }
  
  def splitRpmList(sls: String) = {
    JArray(
      sls.split("\n").map {
        case RpmSplit(pkg, date) => 
          JObject(List(("rpm", JString(pkg)), ("date", JString(date))))
        case x => JString(x)
      }.toList
    )
  }
  
  val BogusName = "(.*[\\W].*)".r
  val dashesToCamel = "(-+([a-z]))".r
  val badChars = "(\\W)".r
  val RpmSplit = "([^\\s]+)\\s+([^\\s].*)".r
  
  def removeBadChars(name: String) = {
    badChars.replaceAllIn(dashesToCamel.replaceAllIn(name, m => s"${(m group 2).toUpperCase}"), "")
  }
}

trait CleaningTransformations extends CleaningHelpers {
  type FieldX = PartialFunction[JField, JField]
  type ValueX = PartialFunction[JValue, JValue]
  
  val sanitizeNames: FieldX = {
    case JField(BogusName(name), v) => JField(removeBadChars(name), v)
  }
  
  val normalizeBooleans: FieldX = {
    case JField(name, JString("yes")) => JField(name, JBool(true))
    case JField(name, JString("no")) => JField(name, JBool(false))
  }
  
  val splitFlags: FieldX = { 
    case JField("flags", JString(s)) => JField("flags", splitValue(s))
  }
  
  // needs to run after sanitizeNames, obvs
  val splitRpms: FieldX = {
    case JField("installedRpms", JString(list)) => {
      JField("installedRpms", splitRpmList(list))
    }
  }
}

object DefaultTransformations extends CleaningTransformations {
  val fieldTransforms = List(sanitizeNames,
    normalizeBooleans,
    splitFlags,
    splitRpms
  )
  
  val valueTransforms = List[ValueX]()
    
  def apply(o: JValue): JValue = {
   val withFields = (o /: fieldTransforms)({(o, x) => o.transformField(x)})
   (withFields /: valueTransforms)({(o, x) => o.transform(x) })
  }
}

object SosReportPreprocessor {
  // TODO:  it would make sense to have custom case classes for these
  import java.io.{File, FileReader, FileWriter}
  import scala.util.{Try, Success, Failure}
  
  def ensureDir(dirname: String): Try[String] = {
    val dir = new File(dirname)
    (dir.exists, dir.isDirectory) match {
      case (true, true) => Success(dirname)
      case (true, false) => Failure(
        new RuntimeException(s"$dirname already exists but is not a directory")
      )
      case (false, _) => Try(Pair(dir.mkdirs(), dirname)._2)
    }
  }
  
  def loadObjects(fn: String): Try[List[JObject]] = {
    val f = new File(fn)
    val parsedFile = Try(parse(new FileReader(f)))
    parsedFile.map(_ match { 
      case JArray(jls) => jls.collect { case j:JObject => j }
      case o: JObject => List(o)
      case _ => List[JObject]()
    })
  }
  
  def partitionByKinds(jls: List[JObject], xform: JObject => JObject = {x => x}): Map[String, Vector[JObject]] = {
    implicit val formats = new org.json4s.DefaultFormats {}
    
    def partitionOne(m: Map[String, Vector[JObject]], jv: JObject) = {
      val kind = (jv \ "_type") match {
        case JString(s) => s
        case _ => "UNKNOWN"
      }
      
      m + ((kind, m.getOrElse(kind, Vector()) :+ xform(jv)))
    }
    
    (Map[String, Vector[JObject]]() /: jls)(partitionOne _)
  }
  
  
}
