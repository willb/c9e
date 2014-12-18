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

import com.redhat.et.c9e.sar.{SarRecord, Metadata, CpuLoad, CpuLoadAll, Memory}

// This file contains some specialized record types to facilitate analyses

trait RecordExtracting[T] {
  def extract(sa: SarRecord): Seq[T]
  def from(srs: Iterable[SarRecord]): Iterable[T] = 
    srs.flatMap(extract)
}

case class CpuLoadEntry(
  nodename: String,
  timestamp: java.util.Date,
  cpu: String,
  idle: Double,
  iowait: Double,
  nice: Double,
  steal: Double,
  system: Double,
  user: Double
  )

case class CpuLoadAllEntry(
  nodename: String,
  timestamp: java.util.Date,
  cpu: String,
  gnice: Double,
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

case class MemoryEntry(
  nodename: String,
  timestamp: java.util.Date,
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
)

object CpuLoadEntry extends RecordExtracting[CpuLoadEntry] {
  def extract(sa: SarRecord) = {
    sa.cpuLoad.map { 
      case CpuLoad(cpu, idle, iowait, nice, steal, system, user) => 
        val ts = sa.timestamp
        CpuLoadEntry(sa._metadata.nodename, ts, cpu, idle, iowait, nice, steal, system, user)
    }
  }
}

object CpuLoadAllEntry extends RecordExtracting[CpuLoadAllEntry] {
  def extract(sa: SarRecord) = {
    sa.cpuLoadAll.map { 
      case CpuLoadAll(cpu, gnice, guest, idle, iowait, irq, nice, soft, steal, system, user) => 
        val ts = sa.timestamp
        CpuLoadAllEntry(sa._metadata.nodename, ts, cpu, gnice.getOrElse(0), guest, idle, iowait, 
          irq, nice, soft, steal, system, user)
    }
  }
}

object MemoryEntry extends RecordExtracting[MemoryEntry] {
  def extract(sa: SarRecord) = {
    Seq(sa.memory match { 
      case Memory(active, buffers, bufpg, cached, campg, commit, commitPercent, 
          dirty, frmpg, inactive, memfree, memused, memusedPercent, swpcad, 
          swpcadPercent, swpfree, swpused, swpusedPercent) => 
        val ts = sa.timestamp
        MemoryEntry(sa._metadata.nodename, ts, active, buffers, bufpg, cached, campg, 
          commit, commitPercent, dirty, frmpg, inactive, memfree, memused, memusedPercent,
          swpcad, swpcadPercent, swpfree, swpused, swpusedPercent)
    })
  }  
}
