ByConity is an open-source column-oriented database management system that allows generating analytical data reports in real time.

## Try ByConity

A minimal ByConity cluster include:
- A [FoundationDB](https://www.foundationdb.org/) database cluster to store meta data.
- A [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) cluster to store data.
- A ByConity server to receive request from clients.
- A ByConity read worker to carry execution of read requests forward from server.
- A ByConity write worker to carry execution of write requests forward from server.
- A ByConity TSO server to provide timestamp
- A ByConity daemon manager to manage background jobs that run in server

We have packed all the setup step inside this docker-compose [file](https://github.com/ByConity/byconity-docker) so you can bring up a test cluster within few commands.
