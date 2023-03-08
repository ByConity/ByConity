import React from "react";
import styles from "./Section.module.css";

function Section(props) {
  const { title = "", children } = props;

  return (
    <section className={styles.section}>
      {!!title && <h2 className={styles.title}>{title}</h2>}

      <div className="container">{children}</div>
    </section>
  );
}

export default Section;
