import React from 'react';
import Translate from '@docusaurus/Translate';

import BilibiliLogo from './bilibili-logo.svg';
import GithubLogo from './github-logo.svg';
import MeetupLogo from './meetup-logo.svg';
import SlackLogo from './slack-logo.svg';
import TwitterLogo from './twitter-logo.svg';
import WechatLogo from './wechat-logo.svg';
import wechatQrPath from './wechat-qr.jpeg';
import DiscordLogo from './discord-logo.svg';

import styles from './CommunitySection.module.scss';
import clsx from 'clsx';

type LogoDisplayProps = {
  image: React.ReactNode;
  title: React.ReactNode;
  externalLink?: string;
  hoverImageSrc?: string;
};

function LogoDisplay(props: LogoDisplayProps) {
  const { image, externalLink, title, hoverImageSrc } = props;

  return (
    <a
      href={externalLink}
      target="_blank"
      className={clsx(styles.logoDisplayLink, {
        [styles.disabled]: !externalLink && !hoverImageSrc,
      })}
    >
      <div className={styles.logoDisplay}>
        <div className={styles.imageContainer}>{image}</div>

        {hoverImageSrc && (
          <div className={styles.hoverImageContainer}>
            <img src={hoverImageSrc} />
          </div>
        )}
      </div>

      <div className={styles.logoTitle}>{title}</div>
    </a>
  );
}

function CommunitySection() {
  return (
    <div className={styles.container}>
      <div className={styles.content}>
        <h2 className={styles.header}>
          <Translate id="homePage.communitySection.title" />
        </h2>

        <div className={styles.logoGrid}>
          <LogoDisplay
            image={<GithubLogo />}
            externalLink="https://github.com/byconity"
            title={<Translate id="homePage.communitySection.github.title" />}
          />

          <LogoDisplay
            image={<WechatLogo />}
            externalLink="https://weixin.qq.com/r/FxP-51HEvQzCrRWE90YF"
            hoverImageSrc={wechatQrPath}
            title={<Translate id="homePage.communitySection.wechat.title" />}
          />

          <LogoDisplay
            image={<BilibiliLogo />}
            externalLink="https://space.bilibili.com/2065226922?spm_id_from=333.1007.0.0"
            title={<Translate id="homePage.communitySection.bilibili.title" />}
          />

          <LogoDisplay
            image={<DiscordLogo />}
            externalLink="https://discord.gg/V4BvTWGEQJ"
            title={<Translate id="homePage.communitySection.discord.title" />}
          />

          <LogoDisplay
            image={<TwitterLogo />}
            title={<Translate id="homePage.communitySection.twitter.title" />}
          />

          <LogoDisplay
            image={<SlackLogo />}
            title={<Translate id="homePage.communitySection.slack.title" />}
          />

          <LogoDisplay
            image={<MeetupLogo />}
            title={<Translate id="homePage.communitySection.meetup.title" />}
          />
        </div>
      </div>
    </div>
  );
}

export default CommunitySection;
