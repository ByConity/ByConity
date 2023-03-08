In this guideline I will set up a Foundation DB cluster on 3 physical machines. They are all using debian OS. I refer to two official guideline here [Getting Started on Linux](https://apple.github.io/foundationdb/getting-started-linux.html) and [Building a Cluster](https://apple.github.io/foundationdb/building-cluster.html). 

Firstly we need to download the binary for the installation in [here](https://github.com/apple/foundationdb/releases/). We need to download the __server__, __monitor__ and __cli__ binary, and those coressponding __sha256__ checksum file . I will chose version __7.1.25__ which is the latest at the time. So let create a folder and download with the following command: 

```
curl -L -o fdbserver.x86_64 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbserver.x86_64
curl -L -o fdbserver.x86_64.sha256 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbserver.x86_64.sha256

curl -L -o fdbmonitor.x86_64 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbmonitor.x86_64
curl -L -o fdbmonitor.x86_64.sha256 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbmonitor.x86_64.sha256

curl -L -o fdbcli.x86_64 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbcli.x86_64
curl -L -o fdbcli.x86_64.sha256 https://github.com/apple/foundationdb/releases/download/7.1.25/fdbcli.x86_64.sha256
```

After download them let do the checksum check on executable files to see if the download are good. For example

```
$ sha256sum --binary fdbserver.x86_64
73b70a75464e64fd0a01a7536e110e31c3e6ce793d425aecfc40f0be9f0652b7 *fdbserver.x86_64

$ cat fdbserver.x86_64.sha256
73b70a75464e64fd0a01a7536e110e31c3e6ce793d425aecfc40f0be9f0652b7  fdbserver.x86_64
```
Assume you download them and store in directory `/root/user_xyz/foundationdb/bin`. 
Next we will delete those sha256 checksum file because we don't need them anymore, we also rename the executation file to remove the trailing `x86_64` and give them executable permission.

```
rm *.sha256
mv fdbcli.x86_64 fdbcli
mv fdbmonitor.x86_64 fdbmonitor
mv fdbserver.x86_64 fdbserver
chmod ug+x fdbcli fdbmonitor fdbserver
``` 

Next we will create some folder to store the config, data and log:
```
mkdir -p /root/user_xyz/fdb_runtime/config
mkdir -p /root/user_xyz/fdb_runtime/data
mkdir -p /root/user_xyz/fdb_runtime/logs
```
Then we create the `foundationdb.conf` config file in `/root/user_xyz/fdb_runtime/config/` with content like this

```
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

Then in the same directory create file `fdb.cluster` with content like this, change the ip to the ip of your machine
```
$ cat /root/user_xyz/fdb_runtime/config/fdb.cluster
clusterdsc:test@example1.host.com:4500
```
We will install FDB as a `systemd` service so in the same folder we will create file `fdb.service` with content like this

```
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

So we have finish prepare the config file. Now let install fdb into `systemd`

Copy the service file into `/etc/systemd/system/`
```
cp fdb.service /etc/systemd/system/
```

Reload the service files to include the new service
```
systemctl daemon-reload
```

Enable and start the service
```
systemctl enable fdb.service
systemctl start fdb.service
```

Check the service and see that it is active
```
$ systemctl status fdb.service
● fdb.service - FoundationDB (KV storage for cnch metastore)
   Loaded: loaded (/etc/systemd/system/fdb.service; disabled; vendor preset: enabled)
   Active: active (running) since Tue 2023-01-17 18:35:42 CST; 20s ago

```

Now I have install fdb service in 1 machine, I will repeate the same for other 2 machine

After install 3 machine, we need to connect them to form a cluster. Now go back to the first node, connect to FDB uusing fdbcli 

```
$ ./foundationdb/bin/fdbcli -C fdb_runtime/config/fdb.cluster
Using cluster file `fdb_runtime/config/fdb.cluster'.

The database is unavailable; type `status' for more information.

Welcome to the fdbcli. For help, type `help'.
fdb>
```
execute this to init a database
```
configure new single ssd
```
Next, execute this to from 2 other nodes to a cluster, replace the address with your machine address 
```
coordinators example1.host.com:4500 example2.host.com:4500 example3.host.com:4500
```

Then exit the cli, you will found that the `fdb.cluster` now have a new content

```
$ cat fdb_runtime/config/fdb.cluster
# DO NOT EDIT!
# This file is auto-generated, it is not to be edited by hand
clusterdsc:wwxVEcyLvSiO3BGKxjIw7Sg5d1UTX5ad@example1.host.com:4500,example2.host.com:4500,example3.host.com:4500
```

Copy this file to other 2 machines and replace the old file then restart fdb service

```
systemctl restart fdb.service
```

Then come back to the first machine and execute this command to change redundant mode to `triple`

```
configure triple
```

Then execute the `status` command with `fdbcli` to see the result, you should see something like this

```
fdb> status

Using cluster file `fdb_runtime/config/fdb.cluster'.

Configuration:
  Redundancy mode        - triple
  Storage engine         - ssd-2
  Coordinators           - 3
  Usable Regions         - 1
```

That's it. You've finished installing Foundationdb server, now you have the `fdb.cluster` file. We will use it in config Byconity.
