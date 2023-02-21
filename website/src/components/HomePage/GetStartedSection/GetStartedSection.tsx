import clsx from "clsx";
import React from "react";
import Link from "@docusaurus/Link";
import { FaGithub } from "react-icons/fa";
import styles from "./GetStartedSection.module.css";
import Section from "@site/src/components/Section";

function GetStartedSection() {
  return (
    <Section>
      <div className="container">
        <div className={clsx("col", styles.card)}>
          <h2>Let's get started!</h2>

          <div>
            <Link
              className="button button--success button--lg margin-right--sm"
              to="/docs/ByConity简介/主要原理概念"
            >
              Get Started
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
      </div>
    </Section>
  );
}

export default GetStartedSection;
