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
    title: <Translate id="homePage.keyFeaturesSection.feature1.title" />,
    icon: <BsSpeedometer2 style={{ color: "var(--ifm-color-danger)" }} />,
    description: (
      <Translate id="homePage.keyFeaturesSection.feature1.description" />
    ),
  },
  {
    title: <Translate id="homePage.keyFeaturesSection.feature2.title" />,
    icon: <MdQueryStats style={{ color: "var(--ifm-color-warning)" }} />,
    description: (
      <Translate id="homePage.keyFeaturesSection.feature2.description" />
    ),
  },
  {
    title: <Translate id="homePage.keyFeaturesSection.feature3.title" />,
    icon: <BiNetworkChart style={{ color: "var(--ifm-color-success)" }} />,
    description: (
      <Translate id="homePage.keyFeaturesSection.feature3.description" />
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
    <Section title={<Translate id="homePage.keyFeaturesSection.title" />}>
      <div className={clsx("container", styles.container)}>
        {FeatureList.map((props, idx) => (
          <Feature key={idx} {...props} />
        ))}
      </div>
    </Section>
  );
}

export default KeyFeaturesSection;
