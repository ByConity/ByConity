import React, { useEffect } from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import KeyFeaturesSection from '@site/src/components/HomePage/KeyFeaturesSection';
import CommunitySection from './CommunitySection';
import IntroductionSection from './IntroductionSection';
import HeroBannerSection from './HeroBannerSection';
import GetStartedSection from './GetStartedSection';

function HomePage() {
  const { siteConfig } = useDocusaurusContext();
  const { title } = siteConfig;

  useEffect(() => {
    const nav = document.querySelector('.navbar');
    nav.classList.add('navbar--no-border');
  }, []);

  return (
    <Layout title={title}>
      <HeroBannerSection />
      <IntroductionSection />
      <KeyFeaturesSection />
      <CommunitySection />
      <GetStartedSection />
    </Layout>
  );
}

export default HomePage;
