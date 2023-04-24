import React from 'react';
import clsx from 'clsx';
import type { Props } from '@theme/Footer/Layout';

export default function FooterLayout({
  style,
  links,
  logo,
  copyright,
}: Props): JSX.Element {
  return (
    <footer
      className={clsx('footer', {
        'footer--dark': style === 'dark',
      })}
    >
      <div className="container">
        <div className="row">
          {logo && <div className="col col--3">{logo}</div>}
          <div className="col">{links}</div>
        </div>
        {copyright && (
          <div className="footer__bottom text--center">{copyright}</div>
        )}
      </div>
    </footer>
  );
}
