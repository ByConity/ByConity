# byconity.github.io

## Developing

This website is powered by [Docusarus](https://docusaurus.io/).
You are encouraged to understand the basics of Docusaurus first.

```bash
npm install # Install NPM dependencies

npm start # Start local server, OR

npm start -l zh-cn # Start the zh-cn server

npm run build # Build the Website
```

## General Folder Structure

```txt
byconity.github.io/
├── docs/ # English docs, use kebab-case
│   ├── category-1
│   │   ├── assets/ # Images used in category-1
│   │   ├── _category_.json # Metadata for category-1
│   │   └──doc-1.md
│   ├── doc-2.md
│   └── doc-3.md
│
├── blog/ # English blogs, use kebab-case
│   ├── 2021-01-01-happy-new-year.md
│   └── 2021-02-14-valentines-day.md
│
├── i18n/
│   └── zh-CN/
│       ├── docusaurus-plugin-content-docs/
│       │   └── current/ # Chinese docs, use kebab-case
│       │       ├── category-1
│       │       │   ├── assets/ # Images used in category-1
│       │       │   ├── _category_.json # Metadata for category-1
│       │       │   └──doc-1.md
│       │       ├── doc-2.md
│       │       └── doc-3.md
│       └── docusaurus-plugin-content-blog/
│           └── current/ # Chinese blogs, use kebab-case
│               ├── 2021-01-01-happy-new-year.md
│               └── 2021-02-14-valentines-day.md
│
├── src/ # Website source code
└── static/
    └── img/
        └── dbyconity-social-card # Opengraph Social Card
```

## For Writers

### File Location

| Type | Language |                                                    Path                                                    |
| :--: | :------: | :--------------------------------------------------------------------------------------------------------: |
| Docs |    en    |                                              [docs/](./docs/)                                              |
| Docs |  zh-cn   | [i18n/zh-cn/docusaurus-plugin-content-docs/current/](./i18n/zh-cn/docusaurus-plugin-content-docs/current/) |
| Blog |    en    |                                              [blog/](./blog/)                                              |
| Blog |  zh-cn   | [i18n/zh-cn/docusaurus-plugin-content-blog/current/](./i18n/zh-cn/docusaurus-plugin-content-blog/current/) |

### Editing Docs

- Learn the basics at https://docusaurus.io/docs/create-doc.
- Docs front matters at https://docusaurus.io/docs/api/plugins/@docusaurus/plugin-content-docs#markdown-front-matter.

### Editing Blog Posts

- Learn the basics at https://docusaurus.io/docs/blog.
- Blog front matters at https://docusaurus.io/docs/api/plugins/@docusaurus/plugin-content-blog#markdown-front-matter
