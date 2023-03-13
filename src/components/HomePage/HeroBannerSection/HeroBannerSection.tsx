import React from "react";
import Link from "@docusaurus/Link";
import Translate from "@docusaurus/Translate";
import { FaGithub } from "react-icons/fa";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import clsx from "clsx";

import styles from "./HeroBannerSection.module.css";

function HeroBannerSection() {
  const { siteConfig } = useDocusaurusContext();
  const { title, tagline } = siteConfig;

  return (
    <header className={clsx("hero hero--primary", styles.hero)}>
      <div className={clsx("container", styles.container)}>
        <h1 className="hero__title">{title}</h1>

        <p className="hero__subtitle">
          <Translate id="tagline" />
        </p>

        <div className={styles.buttons}>
          <Link
            className="button button--success button--lg"
            to="/docs/ByConity简介/主要原理概念"
          >
            <Translate id="getStarted" />
          </Link>
          <Link
            className={clsx(
              "button button--secondary button--lg",
              styles.githubButton
            )}
            href="https://github.com/ByConity/ByConity"
          >
            <FaGithub />
            <span> Github</span>
          </Link>
        </div>
      </div>
    </header>
  );
}

export default HeroBannerSection;
