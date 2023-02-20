import React from "react";
import clsx from "clsx";
import Translate from "@docusaurus/Translate";
import styles from "./styles.module.css";

type FeatureItem = {
  title: JSX.Element;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: <Translate>High Performance, Low Cost</Translate>,
    description: (
      <Translate>
        Support sub-second query response capability under massive data scale
        through vectorized execution engine, columnar storage and CBO+RBO
        optimizer. At the same time, the ultra-high compression ratio helps
        users save a lot of storage space and reduce disk costs.
      </Translate>
    ),
  },
  {
    title: <Translate>Unified Support for Multiple Scenarios</Translate>,
    description: (
      <Translate>
        Supports real-time data streaming and offline batch data writing with
        interactive things capability and multi-table associative query
        capability, which can meet the interactive query needs of online systems
        as well as backend real-time monitoring, report big screen, etc.
      </Translate>
    ),
  },
  {
    title: <Translate>Eco-friendly</Translate>,
    description: (
      <Translate>
        Compatible with most ClickHouse interfaces and tools, supports Kafka,
        Spark, Flink and many other data imports, also supports Superset,
        Tableau and other data visualization tools.
      </Translate>
    ),
  },
];

function Feature({ title, description }: FeatureItem) {
  return (
    <div className={clsx("col col--4")}>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
