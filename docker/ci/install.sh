#!/usr/bin/env bash

# Donwload HDFS
cd /
tar -xvzf hadoop-3.3.4.tar.gz
mv hadoop-3.3.4 hadoop

# Setup HDFS config
cat <<EOT >> ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
EOT
source ~/.bashrc
echo -e "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
sed '$ i\\t<property><name>fs.defaultFS</name><value>hdfs://127.0.0.1:9110</value></property>' -i $HADOOP_HOME/etc/hadoop/core-site.xml
sed '$ i\\t<property><name>dfs.replication</name><value>1</value></property>\n\t<property><name>dfs.permissions</name><value>false</value></property>\n\t<property><name>dfs.name.dir</name><value>file:///hadoop/hadoopdata/hdfs/namenode</value></property>\n\t<property><name>dfs.data.dir</name><value>file:///hadoop/hadoopdata/hdfs/datanode</value></property>' -i $HADOOP_HOME/etc/hadoop/hdfs-site.xml
mkdir -p /hadoop/hadoopdata/hdfs/namenode
mkdir -p /hadoop/hadoopdata/hdfs/datanode

git config --global --add safe.directory /__w/ByConity/ByConity

# Download FDB binaries
# mkdir -p /opt/tiger/foundationdb/bin

