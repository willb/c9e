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

package com.redhat.et.c9e.sar;

case class SarRecord(
  _metadata: Metadata,
  cpuLoad: Array[CpuLoad],
  cpuLoadAll: Array[CpuLoadAll],
  disk: Array[Disk],
  kernel: Kernel,
  memory: Memory,
  swapPages: SwapPages,
  timestamp: java.util.Date
)

case class Metadata(
  fileDate: Option[String], 
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

case class Kernel(
  dentunusd: Int,
  fileNr: Int,
  inodeNr: Int,
  ptyNr: Int
) {
  def this(inodeNr: Int, ptyNr: Int) = this(0, 0, inodeNr, ptyNr)
  def this(fileNr: Int, inodeNr: Int, ptyNr: Int) = this(0, fileNr, inodeNr, ptyNr)
  def this(dentunusd: Int) = this(dentunusd, 0, 0, 0)
  def this() = this(-1, -1, -1, -1)
}

case class Memory(
  active: Int,
  buffers: Int,
  bufpg: Double,
  cached: Int,
  campg: Double,
  commit: Int,
  commitPercent: Double,
  dirty: Int,
  frmpg: Double,
  inactive: Int,
  memfree: Int,
  memused: Int,
  memusedPercent: Double,
  swpcad: Int,
  swpcadPercent: Double,
  swpfree: Int,
  swpused: Int,
  swpusedPercent: Double
) {
//  def this(active: Int,  buffers: Int,   bufpg: Double,   cached: Int,  campg: Double,  commit: Int,  commitPercent: Double,  dirty: Int,  frmpg: Double,  inactive: Int,  memfree: Int,  memused: Int,  memusedPercent: Double,  swpcad: Int,  swpcadPercent: Double,  swpfree: Int,  swpused: Int,  swpusedPercent: Double)

  def this(buffers: Int, bufpg: Double, cached: Int, campg: Double, commit: Int,
      commitPercent: Double, frmpg: Double, inactive: Int, memfree: Int, memused: Int,
      memusedPercent: Double, swpcad: Int, swpcadPercent: Double, swpfree: Int, swpused: Int, 
      swpusedPercent: Double) = 
    this(0,buffers,bufpg,cached,campg,commit,commitPercent,0,frmpg,inactive,memfree,memused,memusedPercent,swpcad,swpcadPercent,swpfree,swpused,swpusedPercent)

  def this(buffers: Int, bufpg: Double, cached: Int, campg: Double, commit: Int,
      commitPercent: Double, frmpg: Double, memfree: Int, memused: Int,
      memusedPercent: Double, swpcad: Int, swpcadPercent: Double, swpfree: Int, swpused: Int, 
      swpusedPercent: Double) = 
    this(0,buffers,bufpg,cached,campg,commit,commitPercent,0,frmpg,0,memfree,memused,memusedPercent,swpcad,swpcadPercent,swpfree,swpused,swpusedPercent)

  def this(bufpg: Double, cached: Int, campg: Double, commit: Int,
      commitPercent: Double, frmpg: Double, memfree: Int, memused: Int,
      memusedPercent: Double, swpcad: Int, swpcadPercent: Double, swpfree: Int, swpused: Int, 
      swpusedPercent: Double) = 
    this(0,0,bufpg,cached,campg,commit,commitPercent,0,frmpg,0,memfree,memused,memusedPercent,swpcad,swpcadPercent,swpfree,swpused,swpusedPercent)
  
  def this() = this(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
}

case class SwapPages(pswpin: Double, pswpout: Double) {
  def this() = this(0, 0)
}
