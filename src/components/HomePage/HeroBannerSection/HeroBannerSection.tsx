import React from "react";
import Link from "@docusaurus/Link";
import Translate from "@docusaurus/Translate";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

import styles from "./HeroBannerSection.module.scss";

function HeroBannerSection() {
  const { siteConfig } = useDocusaurusContext();
  const { title, tagline } = siteConfig;

  return (
    <header className={styles.container}>
      <div className={styles.content}>
        <h1 className={styles.title}>{title}</h1>

        <div className={styles.tagLine}>
          <Translate id="tagline" />
        </div>

        <div>
          <Link
            className={styles.getStartedButton}
            to="/docs/ByConity简介/主要原理概念"
          >
            <Translate id="getStarted" />
          </Link>
        </div>
      </div>
    </header>
  );
}

export default HeroBannerSection;
