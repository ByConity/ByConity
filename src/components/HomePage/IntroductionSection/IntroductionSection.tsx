import React from "react";
import Translate from "@docusaurus/Translate";
import Section from "@site/src/components/Section";
import architectureImagePath from "./architecture.png";
import styles from "./IntroductionSection.module.css";

function IntroductionSection() {
  return (
    <Section title={<Translate id="homePage.introductionSection.title" />}>
      <div className={styles.container}>
        <p className={styles.introductionText}>
          <Translate id="homePage.introductionSection.description" />
        </p>

        <img className={styles.architectureImage} src={architectureImagePath} />
      </div>
    </Section>
  );
}

export default IntroductionSection;
