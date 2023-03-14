One way to deploy ByConity to physical machines is using package manager. For example, install Debian package for Debian OS and rpm packages for Centos OS

ByConity using FoundationDB as meta store, and HDFS as datastore. So before starting to deploy ByConity, we need to deploy FoundationDB and HDFS first.

For deploying [Foundation](https://apple.github.io/foundationdb/) database, you can refer to the installation guide [here](https://github.com/ByConity/ByConity/tree/master/docker/executable_wrapper/FDB_installation.md)

After that we need to deploy an HDFS cluster consist of name node and data node, and create the directory `/user/clickhouse` in HDFS for store data. You can refer to the installation guide [here](https://github.com/ByConity/ByConity/tree/master/docker/executable_wrapper/HDFS_installation.md). After this step, you got the name node url which ussually the value of `fs.defaultFS` that you can find in the `core-site.xml` config. 

Now we will start deploying Byconity. ByConity packages depends on the FoundationDB client package. Hence, before deploying ByConity packages on any machine, we need to deploy FoundationDB client package first. The Foundation client package are tight coupled to version of FoundationDB server. So we need to choose the client package with version that match the version of FoundationDB server

To deploy FoundationDB client package, we go to the release [page](https://github.com/apple/foundationdb/releases), find the right package to your OS and download it. For example, here i download version `7.1.27` for Debian OS, `amd64` machine.
```
curl -L -o foundationdb-clients_7.1.27-1_amd64.deb https://github.com/apple/foundationdb/releases/download/7.1.27/foundationdb-clients_7.1.27-1_amd64.deb
```

Then install with this command
```
sudo dpkg -i foundationdb-clients_7.1.27-1_amd64.deb
```

Next we will deploy ByConity packages, you can find them in release [page](https://github.com/ByConity/ByConity/releases). Or you can build the package by yourself, in that case follow this [guide](https://github.com/ByConity/ByConity/tree/master/docker/packager).
The first package that need to install is the common package `byconity-common-static`, this is the package that all other packages depend on.
```
sudo dpkg -i byconity-common-static_0.1.1.1_amd64.deb
```
After that edit configuration files `/etc/byconity-server/cnch_config.xml` and `/etc/byconity-server/fdb.config`.  The `cnch_config.xml` file contains service_discovery config, hdfs config, foundationdb cluster config path. The `fdb.config` is the cluster config file of FoundationDB cluster. Config it using the guide in [here](https://github.com/ByConity/ByConity/tree/master/packages/config_service_discovery.md)

After that on the machine you want to install TSO service. Download the `byconity-tso` package and install.
```
sudo dpkg -i byconity-tso_0.1.1.1_amd64.deb
```
If this is the first time the package is install on system, it won't start immediately but in next reboot. 
You can check it status by
```
systemctl status byconity-tso`
```
The config for tso service is located in `/etc/byconity-server/byconity-tso.xml`, you can config as you like but the default are good enough, to start it immediately execute
```
systemctl start byconity-tso
```
The next time you install this package again (for example, you want to upgrade), then you don't need to execute `start` command.

With the same manner, in the machine you want to install ByConity server. Download the `byconity-server` package and install.
```
sudo dpkg -i byconity-server_0.1.1.1_amd64.deb 
```
Next is install ByConity resource manager, ByConity worker, Byconity worker write and Byconity daemon manager, download the corresponding packages and install. 
```
sudo dpkg -i byconity-resource-manager_0.1.1.1_amd64.deb 
sudo dpkg -i byconity-worker_0.1.1.1_amd64.deb 
sudo dpkg -i byconity-worker-write_0.1.1.1_amd64.deb 
sudo dpkg -i byconity-daemon-manager_0.1.1.1_amd64.deb 
```
You can install more workers in the same way. Each worker has a settings call `WORKER_ID` in the config file to config `worker id` for worker, `worker id` have to be unique between workers, the default value of `WORKER_ID` in config file is empty. In that case the `worker_id` is automatically assigned to be the IP address of the host machine.

The byconity-resource-manager, byconity-daemon-manger and byconity-tso are light weight service so it could be install in shared machine with other package. But for `byconity-server`, `byconity-worker`, `byconity-worker-write` we should install them in separate machines.
