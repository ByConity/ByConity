// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require("prism-react-renderer/themes/github");
const darkCodeTheme = require("prism-react-renderer/themes/dracula");

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: "ByConity",
  tagline: "云原生大数据分析引擎",
  favicon: "img/favicon.ico",

  // Set the production url of your site here
  url: "https://byconity.github.io",
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: "/",

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: "byconity", // Usually your GitHub org/user name.
  projectName: "byconity.github.io", // Usually your repo name.
  deploymentBranch: "gh-pages",
  trailingSlash: false,

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: "en",
    locales: ["en", "zh-cn"],
  },

  plugins: ["docusaurus-plugin-sass"],

  presets: [
    [
      "classic",
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve("./sidebars.js"),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl: "https://github.com/ByConity/byconity.github.io/tree/main",
          editLocalizedFiles: true,
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl: "https://github.com/ByConity/byconity.github.io/tree/main",
          editLocalizedFiles: true,
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: "img/byconity-social-card.jpg",
      navbar: {
        title: "ByConity",
        hideOnScroll: true,
        logo: {
          alt: "ByConity Logo",
          src: "img/logo.png",
        },
        items: [
          {
            type: "doc",
            docId: "introduction/main-principle-concepts",
            position: "left",
            label: "Docs",
          },
          { to: "/blog", label: "Blog", position: "left" },
          { to: "/community", label: "Community", position: "left" },
          { to: "/users", label: "Users", position: "left" },
          {
            type: "localeDropdown",
            position: "right",
          },
          {
            href: "https://github.com/ByConity/ByConity",
            position: "right",

            // These allows adding Github icon using custom styles
            html: "<span>GitHub</span>",
            className: "header-github-link",
          },
        ],
      },
      footer: {
        style: "dark",
        logo: {
          alt: "ByConity",
          src: "img/footer-logo.svg",
        },
        links: [
          {
            title: "Docs",
            items: [
              {
                label: "Docs",
                to: "/docs/introduction/main-principle-concepts",
              },
            ],
          },
          {
            title: "Community",
            items: [
              {
                label: "Community",
                href: "/community",
              },
              {
                label: "Users",
                href: "/users",
              },
              {
                label: "Bilibili",
                href: "https://space.bilibili.com/2065226922?spm_id_from=333.1007.0.0",
              },
              // {
              //   label: "Twitter",
              //   href: "",
              // },
              // {
              //   label: "Slack",
              //   href: "",
              // },
              // {
              //   label: "Meetup",
              //   href: "",
              // },
            ],
          },
          {
            title: "More",
            items: [
              {
                label: "Blog",
                to: "/blog",
              },
              {
                label: "GitHub",
                href: "https://github.com/ByConity/ByConity",
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} ByteDance.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
      colorMode: {
        disableSwitch: true,
      },
    }),
};

module.exports = config;
