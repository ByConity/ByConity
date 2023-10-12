We are glad to announce that ByConity now support to deploy multiple server/resource manager/tso processes in one cluster for High-availability since https://github.com/ByConity/ByConity/pull/757 is merged. It is implemented according to "Compare And Swap" operation in the shared FoundationDB to determine who is the leader. 


**Upgrade guidelines**


### 1. I do not want to use this feature but I want to upgrade

If you're a previous user of Byconity looking to upgrade, feel free to skip this guide unless you're deploying multiple Byconity clusters using the same FDB service. In that case, for clusters sharing the FDB across different Byconity clusters, additional custom configurations are necessary to prevent them from being treated as replicas.

If you configure the cluster using .yml/.yaml file, then you need to add the following configuration in the cnch-config.yml file in each cluster:
```yaml
service_discovery_kv:
  election_prefix: {your_customized_name_}
```

If you configure the cluster using .xml file, then you need to add the following configuration in cnch-config.xml file under the "yandex" xml tag in each cluster:
```xml
    <service_discovery_kv>
        <election_prefix>{your_customized_name_}</election_prefix>
    </service_discovery_kv>
```

### 2. I want to use this feature and want to increase or decrease the replicas count

In general, you can launch any number of server, resource manager, or TSO service processes based on your requirements. The multi-server will automatically distribute the load of metadata management and visiting using consistent hashing. The multiple resource manager or TSO processes will elect one leader to provide service.

If you're deploying a Byconity cluster using Kubernetes, simply adjust the replica count for each service component, as shown in this example: https://github.com/ByConity/byconity-deploy/blob/master/examples/k8s/value_HA_example.yaml . You have the flexibility to increase or decrease replicas as needed, using "kubectl scale" command.

For manual deployment of the Byconity cluster without Kubernetes support, you can launch extra replica service processes with different listening addresses and log paths for expansion, or stop them for contraction. When the replica count for the Byconity component "server" was changed, remember to update the service discovery information in the cnch-config configuration file, as demonstrated here: https://github.com/ByConity/ByConity/blob/67e2eaab6d5b9c646a336619b2b5500203efc4fd/docker/docker-compose/byconity-multi-cluster/cnch-config.yml . 
