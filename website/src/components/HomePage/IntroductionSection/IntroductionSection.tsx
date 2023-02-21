import clsx from "clsx";
import React from "react";
import Section from "@site/src/components/Section";
import architectureImagePath from "./architecture.png";
import styles from "./IntroductionSection.module.css";

function IntroductionSection() {
  return (
    <Section title="What is ByConity?">
      <div className={styles.container}>
        <p className={styles.introductionText}>
          ByConity is a distributed cloud-native SQL data warehouse engine, that
          excels in interactive queries and Ad-Hoc queries, featuring support
          for querying multiple tables, cluster expansion without sensation, and
          unified aggregation of offline batch data and real-time data streams.
        </p>

        <img className={styles.architectureImage} src={architectureImagePath} />
      </div>
    </Section>
  );
}

export default IntroductionSection;
