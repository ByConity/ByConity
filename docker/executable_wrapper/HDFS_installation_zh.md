
# HDFS 安装指南
在本指南中，我们将在 3 台机器上设置 HDFS，其中 1 台机器用于 namenode，另外 2 台机器用于datanode。这里参考了官方文档 [SingleCluster](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) 和 [ClusterSetup](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html)。 这里将安装 HDFS 版本 3.3.4，因此需要 java-8，因为这是 Hadoop 推荐的 Java 版本。

首先我们在 3 台机器上安装 Java。 有很多方法可以安装 Java，这里使用这两个命令安装：

```
sudo apt-get update
sudo apt-get install openjdk-8-jdk
```

接下来我们需要下载一个hadoop 文件夹，解压并进入

```
$ curl -L -o hadoop-3.3.4.tar.gz https://dlcdn.apache.org/hadoop/common/stable/hadoop-3.3.4.tar.gz
$ tar xvf hadoop-3.3.4.tar.gz
$ ls
hadoop-3.3.4  hadoop-3.3.4.tar.gz
$ cd hadoop-3.3.4
```

然后在文件夹中，我们编辑文件 `etc/hadoop/hadoop-env.sh` 为其设置合适的环境。 这里需要取消注释并修改以下行以设置一些变量。

```
export JAVA_HOME=/usr/lib/jvm/java-8-byteopenjdk-amd64
export HADOOP_HOME=/root/user_xyz/hdfs/hadoop-3.3.4
export HADOOP_LOG_DIR=/root/user_xyz/hdfs/logs
```

接下来用这样的内容编辑文件` etc/hadoop/core-site.xml`。 请注意， `value` tag 将是你的 namenode 地址的值

```
<configuration>
        <property>
                <name>fs.defaultFS</name>
                <value>hdfs://10.119.57.203:12000</value>
        </property>
</configuration>
```

我们已经完成了所有机器的通用设置，从现在开始，namenode 和 datanode 的设置是不同的。 在我们要安装 namenode 的节点中，我们创建一个包含 datanode 列表的文件。 例如，在我的例子中，我创建了 
`datanodes_list.txt`，内容如下

```
$ cat /root/user_xyz/hdfs/datanodes_list.txt
10.11.170.163
10.11.154.30
```

然后创建存放 namenode 运行时数据的目录

```
mkdir -p /root/user_xyz/hdfs/root_data_path_for_namenode
```

接下来编辑文件 `etc/hadoop/hdfs-site.xml` ，内容如下

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

这就是 namenode 的配置。 

现在对于你需要部署 datanode 的那两个节点，创建一个目录来存储 datanode 运行时数据

```
mkdir -p /root/user_xyz/hdfs/root_data_path_for_datanode
```

接下来编辑文件`etc/hadoop/hdfs-site.xml`，内容如下

```
<configuration>
        <property>
                <name>dfs.data.dir</name>
                <value>file:///root/user_xyz/hdfs/root_data_path_for_datanode</value>
        </property>
</configuration>
```

我们已经完成配置，现在转到 namenode 机器，转到hadoop分布格式化文件系统并用这个命令启动 namenode

```
bin/hdfs namenode -format
bin/hdfs  --daemon start namenode
```

然后到另外两台datanode机器，到 hadoop 文件夹，用下面这个命令启动 data node

```
bin/hdfs  --daemon start datanode
```

我们已经完成了 HDFS 的设置。 现在我们必须创建一个目录来存储数据。 所以转到 namenode 机器，从 hadoop 文件夹，执行以下命令

```
bin/hdfs dfs -mkdir -p /user/clickhouse/
bin/hdfs dfs -chown clickhouse /user/clickhouse
bin/hdfs dfs -chmod -R 775 /user/clickhouse
```
