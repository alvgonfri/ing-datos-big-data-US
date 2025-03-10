#!/bin/bash
export HADOOP_CLASSPATH="/opt/hadoop/etc/hadoop"
for f in /opt/hadoop/share/hadoop/common/*.jar; do
    export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$f"
done
for f in /opt/hadoop/share/hadoop/common/lib/*.jar; do
    export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$f"
done
for f in /opt/hadoop/share/hadoop/hdfs/*.jar; do
    export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$f"
done
for f in /opt/hadoop/share/hadoop/hdfs/lib/*.jar; do
    export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$f"
done
for f in /opt/hadoop/share/hadoop/mapreduce/*.jar; do
    export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$f"
done
for f in /opt/hadoop/share/hadoop/mapreduce/lib/*.jar; do
    export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$f"
done
for f in /opt/hadoop/share/hadoop/yarn/*.jar; do
    export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$f"
done
for f in /opt/hadoop/share/hadoop/yarn/lib/*.jar; do
    export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$f"
done