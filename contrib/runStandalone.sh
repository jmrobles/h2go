#!/bin/bash
java -classpath h2.jar org.h2.tools.Server -tcp   -tcpAllowOthers -ifNotExists -trace -baseDir tmpData
