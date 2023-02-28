import React from "react";
import Translate from "@docusaurus/Translate";
import Section from "@site/src/components/Section";
import { RxOpenInNewWindow } from "react-icons/rx";

import bilibiliLogoPath from "./bilibili-logo.png";
import githubLogoPath from "./github-logo.png";
import meetupLogoPath from "./meetup-logo.png";
import slackLogoPath from "./slack-logo.png";
import twitterLogoPath from "./twitter-logo.png";
import wechatLogoPath from "./wechat-logo.png";
import wechatQrPath from "./wechat-qr.jpeg";

import styles from "./CommunitySection.module.scss";

type LogoDisplayProps = {
  imageSrc: string;
  externalLink?: string;
  hoverImageSrc?: string;
};

function LogoDisplay(props: LogoDisplayProps) {
  const { imageSrc, externalLink, hoverImageSrc } = props;

  return (
    <a href={externalLink} target="_blank" className={styles.logoDisplayLink}>
      <div className={styles.logoDisplay}>
        <div className={styles.imageContainer}>
          <img src={imageSrc} />
        </div>
        <div className={styles.hoverImageContainer}>
          {hoverImageSrc ? (
            <img src={hoverImageSrc} />
          ) : (
            <RxOpenInNewWindow size="30%" />
          )}
        </div>
      </div>
    </a>
  );
}

function CommunitySection() {
  return (
    <Section title={<Translate id="homePage.communitySection.title" />}>
      <div className={styles.logoGrid}>
        <LogoDisplay
          imageSrc={githubLogoPath}
          externalLink="https://github.com/byconity"
        />

        <LogoDisplay imageSrc={wechatLogoPath} hoverImageSrc={wechatQrPath} />

        <LogoDisplay
          imageSrc={bilibiliLogoPath}
          externalLink="https://space.bilibili.com/2065226922?spm_id_from=333.1007.0.0"
        />

        <LogoDisplay imageSrc={twitterLogoPath} />

        <LogoDisplay imageSrc={slackLogoPath} />

        <LogoDisplay imageSrc={meetupLogoPath} />
      </div>
    </Section>
  );
}

export default CommunitySection;
