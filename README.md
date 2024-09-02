# Welcome to ByConity

<p align="center">
<img width="717" alt="ByConity Arch 2023" src="https://github.com/ByConity/ByConity/assets/23332032/c266aa89-c1b8-4a35-a47d-ee5718a9443a">

Byconity, an advanced database management system, is a derivative of ClickHouse DBMS, building upon the robust codebase from ClickHouse v21.8. However, Byconity's development path has since diverged, thanks in part to insights gained from Snowflake's architecture.

Our key innovations include the introduction of a compute-storage separation architecture, a state-of-the-art query optimizer, multiple stateless workers, and a shared-storage framework. These enhancements, inspired by both ClickHouse's strength and Snowflake's innovative approach, offer substantial performance and scalability improvements.

We deeply appreciate the profound contributions from the ClickHouse team, with whom we had an early discussion to share our open-source vision and technical implementations. However, given the substantial architectural differences that emerged in our modifications, the ClickHouse team assessed that integrating these changes directly into the original ClickHouse project was not feasible. As a result, we decided to launch Byconity as an independent downstream open-source project. This approach preserves the integrity of both projects while offering distinct solutions for diverse database management needs.


**Query Large Scale Data with Speed and Precision**
When dealing with large-scale data, performance is crucial. Byconity shines in this aspect by providing powerful querying capabilities that excel in large-scale environments. With Byconity, you can extract valuable insights from vast amounts of data quickly and accurately.

**Break Down Data Silos with Byconity**
Data silos pose significant challenges in data management. With different systems and processes often resulting in isolated islands of data, it hampers data analysis and insights. Byconity addresses this issue by seamlessly ingesting both batch-loaded data and streaming data, thus enabling your systems to break down silos for smoother data flow.

**Designed for the Cloud, Flexible for Your Needs**
Byconity is designed with a cloud-native approach, optimized to take full advantage of the cloud's scalability, resilience, and ease of deployment. It can work seamlessly on both Kubernetes clusters and physical clusters, offering you the flexibility to deploy in the environment that best meets your requirements. This broad compatibility ensures that you can leverage Byconity's benefits, irrespective of your infrastructure.

## Benefits
- **Unified Data Management**: Byconity eliminates the need to maintain separate processes for batch and streaming data, making your systems more efficient.
- **High-Performance Data Querying** : Byconity's robust querying capabilities allow for quick and accurate data retrieval from large-scale datasets.
- **Avoid Data Silos** : By handling both batch and streaming data, Byconity ensures all your data can be integrated, promoting better insights.
- **Cloud-Native Design** : Byconity is built with a cloud-native approach, allowing it to efficiently leverage the advantages of the cloud and work seamlessly on both Kubernetes and physical clusters.
- **Open Source**: Being an open-source project, Byconity encourages community collaboration. You can contribute, improve, and tailor the platform according to your needs.

## Build and Run ByConity

The easiest way to build ByConity is built in [docker dev-env](https://github.com/ByConity/ByConity/tree/master/docker/debian/dev-env). If you build on your local machine, the ByConity executable file depends on the Foundation DB library `libfdb_c.so`. So to run it, we need to install the FoundationDB client package. This [link](https://apple.github.io/foundationdb/getting-started-linux.html) tells how to install. We can download the client package from FoundationDB GitHub release pages, for example [here][foundationdb-client-library].

In case you want to build ByConity in the metal machine, follow this [guide](https://github.com/ByConity/ByConity/tree/master/doc/build_in_metal_machine.md)

[foundationdb-client-library]: https://github.com/apple/foundationdb/releases/tag/7.1.3

Using [Docker Compose](./docker/docker-compose/README.md) would be convenient for running a ByConity cluster.

## Useful Links

- [Official Website](https://byconity.github.io/): has a quick high-level overview of ByConity on the home page.
- [Documentation](https://byconity.github.io/docs/introduction/main-principle-concepts): introduce basic usage guide and tech deep dive.
- [Getting started with Kubernetes](https://byconity.github.io/docs/deployment/deployment-with-k8s): demonstrates how to deploy a ByConity cluster in your Kubernetes clusters.
- [Getting started with physical machines](https://byconity.github.io/docs/deployment/package-deployment): demonstrates how to deploy ByConity in your physical clusters.
- [Contribution Guideline](https://github.com/ByConity/ByConity/blob/master/CONTRIBUTING.md): Welcome you to join ByConity developer group and list some tips for fresh joiners to be quickly hands-on.
- **Contact Us** : you can easily find us in [Discord server](https://discord.gg/V4BvTWGEQJ), [Youtube Channel](https://www.youtube.com/@ByConity/featured) and [Twitter](https://twitter.com/ByConity)
