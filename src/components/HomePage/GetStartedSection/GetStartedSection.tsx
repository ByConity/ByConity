import React from "react";
import Translate from "@docusaurus/Translate";
import Link from "@docusaurus/Link";
import styles from "./GetStartedSection.module.scss";
import FileIcon from "./file.svg";
import CodeIcon from "./command-line.svg";

function GetStartedSection() {
  return (
    <div className={styles.container}>
      <div className={styles.content}>
        <div>
          <h2 className={styles.header}>
            <Translate id="homePage.getStartedSection.title" />
          </h2>
        </div>

        <div className={styles.buttons}>
          <Link className={styles.button} to="/docs/ByConity简介/主要原理概念">
            <FileIcon />
            <Translate id="homePage.getStartedSection.readDocs.title" />
          </Link>

          <Link
            className={styles.button}
            href="https://github.com/ByConity/ByConity"
          >
            <CodeIcon />
            <Translate id="homePage.getStartedSection.contribute.title" />
          </Link>
        </div>
      </div>
    </div>
  );
}

export default GetStartedSection;
