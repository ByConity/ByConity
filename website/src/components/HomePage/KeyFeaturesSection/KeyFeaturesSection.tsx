import React from "react";
import clsx from "clsx";
import Translate from "@docusaurus/Translate";
import Section from "@site/src/components/Section";
import { BsSpeedometer2 } from "react-icons/bs";
import { BiNetworkChart } from "react-icons/bi";
import { MdQueryStats } from "react-icons/md";
import styles from "./KeyFeaturesSection.module.css";

type FeatureItem = {
  className?: string;
  title: JSX.Element;
  icon: React.ReactNode;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: <Translate>High Performance, Low Cost</Translate>,
    icon: <BsSpeedometer2 style={{ color: "var(--ifm-color-danger)" }} />,
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
    icon: <MdQueryStats style={{ color: "var(--ifm-color-warning)" }} />,
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
    icon: <BiNetworkChart style={{ color: "var(--ifm-color-success)" }} />,
    description: (
      <Translate>
        Compatible with most ClickHouse interfaces and tools, supports Kafka,
        Spark, Flink and many other data imports, also supports Superset,
        Tableau and other data visualization tools.
      </Translate>
    ),
  },
];

function Feature({ className, title, description, icon }: FeatureItem) {
  return (
    <div className={clsx(styles.card, className)}>
      <div className={styles.cardImage}>{icon}</div>

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
