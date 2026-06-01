# ByConity EOL
Dear ByConity Users,

We would like to share an important update regarding the future of the ByConity project. 

ByConity is an open-source cloud-native data warehouse project developed by Volcano Engine and built upon the ClickHouse 21.8 codebase. Since its inception, ByConity has served as an important platform for exploring cloud-native data warehouse architectures within ByteHouse.
As the ByteHouse platform has continued to evolve, however, the development paths of ByteHouse and ByConity have gradually diverged. After careful consideration, we have decided to transition ByConity into retirement and conclude active development of the open-source project.

According to the current plan, the project will enter a retirement transition period starting on **June 1, 2026**. During this period, we will continue to address potential security vulnerabilities and ensure the stability of core functionality, but no new features will be actively developed.
After **August 1, 2026**, the ByConity GitHub repository will be archived and switched to read-only mode for reference purposes. Once the repository enters archive status, the ByteHouse team will no longer provide active maintenance, feature enhancements, routine bug fixes, or security patches for ByConity. The ByConity website and documentation portal are expected to remain available for a period of time to support migration efforts; however, they will no longer receive updates from the ByteHouse team.

At present, there is no direct one-to-one open-source replacement for ByConity. For organizations that prioritize continuously evolving cloud data warehouse capabilities, enterprise-grade service guarantees, and fully managed operations, migration to **Volcano Engine ByteHouse** may be worth evaluating.

Finally, the ByteHouse team would like to express its sincere gratitude to all developers, users, and community members who have contributed to ByConity. We would also like to thank the ClickHouse community for its long-standing contributions to database kernel development and analytical query systems.

If you have questions regarding this transition, migration planning, community maintenance efforts, or ByteHouse product offerings, please contact us at: byconity@bytedance.com.

Sincerely,

The Volcano Engine ByteHouse Team

# ByConity 退休公告
尊敬的ByConity 用户们：

火山引擎 ByteHouse ByConity 是一个开源云原生数据仓库项目，源自 ClickHouse 21.8 代码基础。该项目面向大规模数据场景，提供了交互式查询、Ad-Hoc 分析、批流一体数据接入以及云原生部署能力。  

作为独立开源项目，ByConity 早期承载了 ByteHouse 云原生数仓架构探索。然而，随着 ByteHouse 产品体系持续演进，二者在工程节奏、发布机制、运维体系和用户支持模式上已经形成差异。经过审慎评估，我们计划停止对 ByConity 开源项目的主动维护。这并不意味着我们否定 ByConity 的技术价值。相反，ByConity 在存算分离、云原生部署和大规模分析查询方向积累了不少有价值的实践，也为后续云原生数据仓库产品和技术演进提供了重要参考。

根据当前规划，**2026年06月01日**起，项目将进入退休过渡期。在退休过渡期内，我们仍会对 ByConity 可能的漏洞进行安全修复，确保其基本功能稳定运行，但不再主动新增功能。**2026年08月01日**后， ByConity 的 GitHub 仓库将被设置为只读存档状态，供用户查阅。在进入存档状态后，ByteHouse 团队将不再为 ByConity 提供主动维护、功能更新、常规漏洞修复或安全补丁。ByConity 官网与文档站点预计还会保留一段时间，用于迁移参考，但ByteHouse 团队将不再对官网和文档进行更新。

目前，ByConity 并没有一对一的直接替代开源项目。对于看重持续演进的云数仓能力、企业级服务保障和托管化运维的用户，可以评估迁移至火山引擎 ByteHouse。

最后，Bytehouse 团队衷心感谢所有为 ByConity 做出贡献的开发者、用户和社区成员，也感谢 ClickHouse 社区在数据库内核和分析型查询系统方向的长期贡献。ByConity 从内部技术探索走向开源社区，离不开大家在代码贡献、Issue 反馈、部署实践、文档改进和技术讨论中的持续投入，我们也希望这些经验能够继续为后续的数据仓库和云原生分析系统建设提供参考。
如对本次过渡、迁移安排或社区维护意向有疑问，或希望了解 ByteHouse 商业产品能力与迁移可能性的用户，可通过byconity@bytedance.com 联系我们。

火山引擎 ByteHouse 团队敬上


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
