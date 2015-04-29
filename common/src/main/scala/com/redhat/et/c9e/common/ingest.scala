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
 * limitations under the License.
 */

package com.redhat.et.c9e.common;

import com.redhat.et.silex.app.AppCommon

class SosReportIngest[A <: AppCommon](dataDir: String, app: A) {
  lazy val cmdline = app.sqlContext.jsonFile(s"$dataDir/cmdline")
  lazy val cpuinfo = app.sqlContext.jsonFile(s"$dataDir/cpuinfo")
  lazy val date = app.sqlContext.jsonFile(s"$dataDir/date")
  lazy val dmidecode = app.sqlContext.jsonFile(s"$dataDir/dmidecode")
  lazy val installedRpms = app.sqlContext.jsonFile(s"$dataDir/installed-rpms")
  lazy val lsblk = app.sqlContext.jsonFile(s"$dataDir/lsblk")
  lazy val lsmod = app.sqlContext.jsonFile(s"$dataDir/lsmod")
  lazy val lspci = app.sqlContext.jsonFile(s"$dataDir/lspci")
  lazy val meminfo = app.sqlContext.jsonFile(s"$dataDir/meminfo")
  lazy val ps = app.sqlContext.jsonFile(s"$dataDir/ps")
  lazy val slabinfo = app.sqlContext.jsonFile(s"$dataDir/slabinfo")
  lazy val vmstat = app.sqlContext.jsonFile(s"$dataDir/vmstat")
}

class SarSqlIngest[A <: AppCommon](dataDir: String, app: A) {
  import com.redhat.et.c9e.sar.analysis._

  /** suitable for SQL, can be registered as a table, etc. */
  lazy val sar = app.sqlContext.jsonFile(s"$dataDir/sar")
}

class SarIngest[A <: AppCommon](args: Array[String], app: A) {
  import com.redhat.et.c9e.sar.analysis._

  /** raw case-class records */
  lazy val records = LazySarConverter.run(args).toIterable

  /** specialized cpu load records */
  lazy val cpuLoadEntries = app.context.parallelize(CpuLoadEntry.from(records).toSeq)

  /** specialized cpu-load-all records */
  lazy val cpuLoadAllEntries = app.context.parallelize(CpuLoadAllEntry.from(records).toSeq)
  
  /** specialized memory records */
  lazy val memoryEntries = app.context.parallelize(MemoryEntry.from(records).toSeq)
}
