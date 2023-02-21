import React from "react";
import clsx from "clsx";
import Translate from "@docusaurus/Translate";
import styles from "./KeyFeaturesSection.module.css";
import Section from "@site/src/components/Section";

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
    <div className={styles.card}>
      <div>
        <h3 className={styles.cardHeader}>{title}</h3>
      </div>
      <div className={styles.cardBody}>{description}</div>
    </div>
  );
}

function KeyFeaturesSection() {
  return (
    <Section title="Key Features">
      <div className={clsx("container", styles.container)}>
        {FeatureList.map((props, idx) => (
          <Feature key={idx} {...props} />
        ))}
      </div>
    </Section>
  );
}

export default KeyFeaturesSection;
