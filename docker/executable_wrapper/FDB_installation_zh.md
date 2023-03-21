# Foundation DB 安装指南
在本指南中，我将在3台使用Debian OS的物理机上设置一个FoundationDB集群。 可以参考[Getting Started on Linux](https://apple.github.io/foundationdb/getting-started-linux.html) 和 [Building a Cluster](https://apple.github.io/foundationdb/building-cluster.html)两个官方指南。

首先，我们需要在[此处](https://github.com/apple/foundationdb/releases/)下载并安装的二进制文件。 我们需要下载 **server**, **monitor**，和**cli** 安装包，以及校验文件，请保证安装包的版本都是匹配的。 接着创建一个文件夹并使用以下命令下载：

```Plaintext
curl -L -o fdbserver.x86_64 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbserver.x86_64
curl -L -o fdbserver.x86_64.sha256 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbserver.x86_64.sha256

curl -L -o fdbmonitor.x86_64 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbmonitor.x86_64
curl -L -o fdbmonitor.x86_64.sha256 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbmonitor.x86_64.sha256

curl -L -o fdbcli.x86_64 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbcli.x86_64
curl -L -o fdbcli.x86_64.sha256 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbcli.x86_64.sha256
```

下载后，让我们在可执行文件上进行checksum check以查看下载是否有误，例如：

```Plaintext
$ sha256sum --binary fdbserver.x86_64
73b70a75464e64fd0a01a7536e110e31c3e6ce793d425aecfc40f0be9f0652b7 *fdbserver.x86_64

$ cat fdbserver.x86_64.sha256
73b70a75464e64fd0a01a7536e110e31c3e6ce793d425aecfc40f0be9f0652b7  fdbserver.x86_64
```

假设它们被存储在目录/`/root/user_xyz/foundationdb/bin`中。 接下来删除那些sha256 checksum文件，因为这些文件将不再被使用。我们可以重命名可执行文件并授予它们可执行的权限。

```Plaintext
rm *.sha256
mv fdbcli.x86_64 fdbcli
mv fdbmonitor.x86_64 fdbmonitor
mv fdbserver.x86_64 fdbserver
chmod ug+x fdbcli fdbmonitor fdbserver
```

接下来我们将创建一些文件夹来存储配置、数据和日志：

```Plaintext
mkdir -p /root/user_xyz/fdb_runtime/config
mkdir -p /root/user_xyz/fdb_runtime/data
mkdir -p /root/user_xyz/fdb_runtime/logs
```

在 `/root/user_xyz/fdb_runtime/config/` 中创建 `foundationdb.conf` 配置文件，内容如下：

```Plaintext
$ cat /root/user_xyz/fdb_runtime/config/foundationdb.conf
[fdbmonitor]
user = root

[general]
cluster-file = /root/user_xyz/fdb_runtime/config/fdb.cluster
restart-delay = 60

[fdbserver]

command = /root/user_xyz/foundationdb/bin/fdbserver
datadir = /root/user_xyz/fdb_runtime/data/$ID
logdir = /root/user_xyz/fdb_runtime/logs/
public-address = auto:$ID
listen-address = public


[fdbserver.4500]
class=stateless
[fdbserver.4501]
class=transaction
[fdbserver.4502]
class=storage
[fdbserver.4503]
class=stateless
```

在同一目录下创建 `fdb.cluster` 文件，内容如下，修改ip为你机器的ip。

```Plaintext
$ cat /root/user_xyz/fdb_runtime/config/fdb.cluster
clusterdsc:test@10.25.160.40:4500
```

我们将把 FDB 安装为 `systemd` 服务，因此在同一个文件夹中我们将创建`fdb.service`文件，其内容如下：

```Plaintext
$ cat /root/user_xyz/fdb_runtime/config/fdb.service
[Unit]
Description=FoundationDB (KV storage for cnch metastore)

[Service]
Restart=always
RestartSec=30
TimeoutStopSec=600
ExecStart=/root/user_xyz/foundationdb/bin/fdbmonitor --conffile /root/user_xyz/fdb_runtime/config/foundationdb.conf --lockfile /root/user_xyz/fdb_runtime/fdbmonitor.pid

[Install]
WantedBy=multi-user.target
```

这样我们就完成了配置文件的准备。 现在让我们将 fdb 安装到 `systemd` 中。

将服务文件复制到 `/etc/systemd/system/`。

```Plaintext
cp fdb.service /etc/systemd/system/
```

重新加载服务文件以包含新服务。

```Plaintext
systemctl daemon-reload
```

启动服务。

```Plaintext
systemctl enable fdb.service
systemctl start fdb.service
```

检查服务并查看它是否处于活动状态。

```Plaintext
$ systemctl status fdb.service
● fdb.service - FoundationDB (KV storage for cnch metastore)
   Loaded: loaded (/etc/systemd/system/fdb.service; disabled; vendor preset: enabled)
   Active: active (running) since Tue 2023-01-17 18:35:42 CST; 20s ago
```

现在我们已经在 1 台机器上安装了 fdb 服务，下面将在其他 2 台机器上重复同样的操作。

安装3台机器后，我们需要将它们连接起来形成一个集群。 现在回到第一个节点，使用 fdbcli 连接到 FDB。

```Plaintext
$ ./foundationdb/bin/fdbcli -C fdb_runtime/config/fdb.cluster
Using cluster file `fdb_runtime/config/fdb.cluster'.

The database is unavailable; type `status' for more information.

Welcome to the fdbcli. For help, type `help'.
fdb>
```

执行此以初始化数据库。

```Plaintext
configure new single ssd
```

接下来，执行此从其他 2 个节点到一个集群，将地址替换为您的机器地址。

```Plaintext
coordinators 10.149.54.214:4500 10.21.154.30:4500 10.21.170.163:4500
```

然后退出cli，你会发现`fdb.cluster` 现在有了新的内容。

```Plaintext
$ cat fdb_runtime/config/fdb.cluster
# DO NOT EDIT!
# This file is auto-generated, it is not to be edited by hand
clusterdsc:wwxVEcyLvSiO3BGKxjIw7Sg5d1UTX5ad@10.21.154.30:4500,10.21.170.163:4500,10.149.54.214:4500
```

将此文件复制到其他2台机器并替换旧文件然后重新启动fdb服务。

```Plaintext
systemctl restart fdb.service
```

然后回到第一台机器，执行这条命令。

```Plaintext
configure triple
```

然后用 `fdbcli`执行 `status` 命令看看结果，你应该会看到以下内容：

```Plaintext
fdb> status

Using cluster file `fdb_runtime/config/fdb.cluster'.

Configuration:
  Redundancy mode        - triple
  Storage engine         - ssd-2
  Coordinators           - 3
  Usable Regions         - 1
```

到这里已经完成了 Foundationdb 服务器的安装，现在您有了`fdb.cluster` 文件，我们将在配置 Byconity 中使用它。
