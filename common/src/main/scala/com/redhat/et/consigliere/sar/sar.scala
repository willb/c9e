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
 * limitations under the License.c
 */

package com.redhat.et.consigliere.sar;

case class SarRecord(
  _metadata: Metadata,
  cpuLoad: Array[CpuLoad],
  cpuLoadAll: Array[CpuLoadAll],
  disk: Array[Disk]
)

case class Metadata(
  fileDate: String, 
  generatedBy: String, 
  generatedByVersion: String, 
  machine: String, 
  nodename: String, 
  numberOfCpus: Int, 
  sysdataVersion: Double, 
  sysname: String
)


//Array
case class CpuLoad(
  cpu: String,
  idle: Double,
  iowait: Double,
  nice: Double,
  steal: Double,
  system: Double,
  user: Double
)

// Array
case class CpuLoadAll(
  cpu: String,
  gnice: Option[Double],
  guest: Double,
  idle: Double,
  iowait: Double,
  irq: Double,
  nice: Double,
  soft: Double,
  steal: Double,
  sys: Double,
  usr: Double
)

// Array
case class Disk(
  avgquSz: Double,
  avgrqSz: Double,
  await: Double,
  diskDevice: String,
  rd_sec: Double,
  svctm: Double,
  tps: Double,
  utilPercent: Double,
  wr_sec: Double
)