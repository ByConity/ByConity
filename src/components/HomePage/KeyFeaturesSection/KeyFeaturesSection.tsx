import React from 'react';
import clsx from 'clsx';
import Translate from '@docusaurus/Translate';
import EcoModeIcon from './eco-mode.svg';
import ExternalIcon from './external.svg';
import FaceIcon from './face.svg';
import styles from './KeyFeaturesSection.module.scss';

type FeatureCardProps = {
  title: JSX.Element;
  icon: React.ReactNode;
  description: JSX.Element;
};

function FeatureCard(props: FeatureCardProps) {
  const { title, description, icon } = props;

  return (
    <div className={styles.card}>
      <div className={styles.cardHeader}>
        <div className={styles.cardIcon}>{icon}</div>
        <h3 className={styles.cardTitle}>{title}</h3>
      </div>

      <div className={styles.cardBody}>{description}</div>
    </div>
  );
}

function KeyFeaturesSection() {
  return (
    <div className={styles.container}>
      <div className={styles.content}>
        <h2 className={styles.header}>
          <Translate id="homePage.keyFeaturesSection.title" />
        </h2>

        <FeatureCard
          title={<Translate id="homePage.keyFeaturesSection.feature1.title" />}
          icon={<EcoModeIcon />}
          description={
            <Translate id="homePage.keyFeaturesSection.feature1.description" />
          }
        />
        <FeatureCard
          title={<Translate id="homePage.keyFeaturesSection.feature2.title" />}
          icon={<ExternalIcon />}
          description={
            <Translate id="homePage.keyFeaturesSection.feature2.description" />
          }
        />
        <FeatureCard
          title={<Translate id="homePage.keyFeaturesSection.feature3.title" />}
          icon={<FaceIcon />}
          description={
            <Translate id="homePage.keyFeaturesSection.feature3.description" />
          }
        />
      </div>
    </div>
  );
}

export default KeyFeaturesSection;
