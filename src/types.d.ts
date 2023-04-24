declare module '*.png' {
  const value: string;
  export = value;
}

declare module '*.jpeg' {
  const value: string;
  export = value;
}

declare module '*.scss' {
  const content: Record<string, string>;
  export default content;
}
