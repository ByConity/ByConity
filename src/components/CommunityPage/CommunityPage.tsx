import React from 'react';
import Layout from '@theme/Layout';
import Translate, { translate } from '@docusaurus/Translate';
import { MdConstruction } from 'react-icons/md';

function UnderConstruction() {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        marginTop: 128,
      }}
    >
      <div style={{ fontSize: '5rem' }}>
        <MdConstruction />
      </div>
      <div>
        <Translate id="underConstruction.text" />
      </div>
    </div>
  );
}

function CommunityPage() {
  return (
    <Layout title={translate({ id: 'communityPage.title' })}>
      <main>
        <UnderConstruction />
      </main>
    </Layout>
  );
}

export default CommunityPage;
