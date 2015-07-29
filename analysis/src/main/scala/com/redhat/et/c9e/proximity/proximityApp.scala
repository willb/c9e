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
import org.elasticsearch.spark._

object ProxApp extends AppCommon {
  addConfig { conf =>
    conf
     .set("es.nodes", sys.env.getOrElse("ES_NODES", "localhost"))
     .set("es.nodes.discovery", "false")
  }

  override def appName = "Machine role analysis"

  def appMain(args: Array[String]) {
    args match {
      case Array(index, indexType) =>
        val resource = s"$index/$indexType"
        val count = context.esRDD(resource).count()
        Console.println(s"$resource has $count documents")
      case _ =>
        Console.println("usage: IndexCount index type")
    }
  }
}