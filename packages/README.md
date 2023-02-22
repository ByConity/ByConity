One way to deploy ByConity to physical machines is to some package corresponding to the package manager of the OS. For example, debian package for Debian OS and rpm packages for Centos OS

For all machine we want to deploy ByConity
The first step we install the FoundationDB client package that match with FoundationDB server package
curl -L -o foundationdb-clients_7.1.27-1_amd64.deb https://github.com/apple/foundationdb/releases/download/7.1.27/foundationdb-clients_7.1.27-1_amd64.deb

Install fdb-client
sudo dpkg -i foundationdb-clients_7.1.27-1_amd64.deb


Install static package contains binary
sudo dpkg -i byconity-common-static_0.1.1.1_amd64.deb
After that edit the file /etc/byconity-server/cnch_config.xml and /etc/byconity-server/fdb.config 



Edit /etc/byconity-server/byconity-tso.xml and /etc/byconity-server/cnch_config.xml as guideline in https://github.com/ByConity/ByConity/packages/ then the tso service will be ready soon, check readiness with systemctl status byconity-tso

sudo dpkg -i byconity-server_0.1.1.1_amd64.deb

edit /etc/byconity-server/cnch_config.xml

edit /etc/byconity-server/fdb.cluster
