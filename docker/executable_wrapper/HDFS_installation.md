In this guide I will set up HDFS on 3 machine, 1 machine is for name node and other 2 machines is for data nodes. I refer to the following official document [SingleCluster](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) and [ClusterSetup](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html). I will install HDFS version 3.3.4 so i need java-8 because this is recommended java version for this Hadoop

Firstly we install Java in 3 machines. There are many ways to install java but i install with this two commands
```
sudo apt-get update
sudo apt-get install openjdk-8-jdk
```

Next we need download a hadoop distribution, extract it and go inside
```
$ curl -L -o hadoop-3.3.4.tar.gz https://dlcdn.apache.org/hadoop/common/stable/hadoop-3.3.4.tar.gz
$ tar xvf hadoop-3.3.4.tar.gz
$ ls
hadoop-3.3.4  hadoop-3.3.4.tar.gz
$ cd hadoop-3.3.4
```

Then inside the distribution we edit file `etc/hadoop/hadoop-env.sh` to set suitable env for it . I need to go uncomment and modify the below lines to set some variable. 
```
export JAVA_HOME=/usr/lib/jvm/java-8-byteopenjdk-amd64
export HADOOP_HOME=/root/user_xyz/hdfs/hadoop-3.3.4
export HADOOP_LOG_DIR=/root/user_xyz/hdfs/logs
```

Next edit the file `etc/hadoop/core-site.xml` with content like this. Note that the `value` tag will be the value of your name node address

```
<configuration>
        <property>
                <name>fs.defaultFS</name>
                <value>hdfs://example1.host.com:12000</value>
        </property>
</configuration>
```

So we have finish the common setup for all there machines, from now the setup is different for namenode and datanode.
In the node that we want to install namenode, we create a file contain list of datanode. For example, in my case I create `datanodes_list.txt` with content like this

```
$ cat /root/user_xyz/hdfs/datanodes_list.txt
example2.host.com
example3.host.com
```

Then create a directory for storing namenode runtime data
```
mkdir -p /root/user_xyz/hdfs/root_data_path_for_namenode
```

Next edit file `etc/hadoop/hdfs-site.xml` with content like this
```
<configuration>
        <property>
                <name>dfs.replication</name>
                <value>1</value>
        </property>
        <property>
                <name>dfs.namenode.name.dir</name>
                <value>file:///root/user_xyz/hdfs/root_data_path_for_namenode</value>
        </property>
        <property>
                <name>dfs.hosts</name>
                <value>/root/user_xyz/hdfs/datanodes_list.txt</value>
        </property>

</configuration>
```

That's it for namenode. Now for those two nodes that you need to deploy data node,
create a directory for store datanode runtime data
```
mkdir -p /root/user_xyz/hdfs/root_data_path_for_datanode
```

Next edit file `etc/hadoop/hdfs-site.xml` with content like this

```
<configuration>
        <property>
                <name>dfs.data.dir</name>
                <value>file:///root/user_xyz/hdfs/root_data_path_for_datanode</value>
        </property>
</configuration>
```

We have finish the configuration, now go to the namenode machine, go to the hadoop distribution
Format the file system and start namenode with this command

```
bin/hdfs namenode -format
bin/hdfs  --daemon start namenode
```

Then go to other two data node machines, go to the hadoop distribution and start data node with this command
```
bin/hdfs  --daemon start datanode
```

We have finished setup HDFS. Now we will have to create a directory to store data. So go to the namenode machine, from hadoop distribution, execute follow commands
```
bin/hdfs dfs -mkdir -p /user/clickhouse/
bin/hdfs dfs -chown clickhouse /user/clickhouse
bin/hdfs dfs -chmod -R 775 /user/clickhouse
```

