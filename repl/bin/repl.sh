#!/bin/sh

# set SBT to the location of a current sbt-extras script
export SBT=${SBT:-./sbt}

${SBT} package && java $C9E_JVM_OPTS -cp $(${SBT} -batch -q "export repl/compile:dependencyClasspath" | tail -1) com.redhat.et.c9e.common.ReplApp
stty sane
