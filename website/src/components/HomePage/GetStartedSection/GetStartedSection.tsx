import clsx from "clsx";
import React from "react";
import Translate from "@docusaurus/Translate";
import Link from "@docusaurus/Link";
import { FaGithub } from "react-icons/fa";
import styles from "./GetStartedSection.module.css";
import Section from "@site/src/components/Section";
import { BsBook, BsCodeSlash } from "react-icons/bs";

function Card(props) {
  const { title, description, icon } = props;

  return (
    <div className={styles.card}>
      <div className={styles.cardImage}>{icon}</div>

      <div>
        <h3 className={styles.cardHeader}>{title}</h3>
      </div>

      <div className={styles.cardBody}>{description}</div>
    </div>
  );
}

function GetStartedSection() {
  return (
    <Section title={<Translate id="homePage.getStartedSection.title" />}>
      <div className={clsx("container", styles.container)}>
        <Card
          title={<Translate id="homePage.getStartedSection.readDocs.title" />}
          icon={<BsBook />}
          description={
            <Link
              className="button button--success button--lg margin-right--sm"
              to="/docs/ByConity简介/主要原理概念"
            >
              <Translate id="getStarted" />
            </Link>
          }
        />

        <Card
          title={<Translate id="homePage.getStartedSection.contribute.title" />}
          icon={<BsCodeSlash />}
          description={
            <Link
              className={clsx(
                "button button--success button--lg",
                styles.githubButton
              )}
              href="https://github.com/ByConity/ByConity"
            >
              <FaGithub />
              <span> Github</span>
            </Link>
          }
        />
      </div>
    </Section>
  );
}

export default GetStartedSection;
