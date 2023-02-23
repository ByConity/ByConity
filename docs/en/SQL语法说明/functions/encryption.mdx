---
title: "Encryption"
slug: "encryption"
hidden: true
createdAt: "2021-07-29T12:04:18.067Z"
updatedAt: "2021-09-23T03:58:33.487Z"
tags:
  - Docs
---

> Notice:
> Some of the examples below are referenced from [ClickHouse Documentation](https://clickhouse.com/docs/en/sql-reference/functions/) but have been adapted and modified to work in ByConity.

:::danger
no encrypt functions but we have decrypt functions. syntax unsure
:::

## decrypt

This function decrypts ciphertext into a plaintext using these modes:

- aes-128-ecb, aes-192-ecb, aes-256-ecb

- aes-128-cbc, aes-192-cbc, aes-256-cbc

- aes-128-cfb1, aes-192-cfb1, aes-256-cfb1

- aes-128-cfb8, aes-192-cfb8, aes-256-cfb8

- aes-128-cfb128, aes-192-cfb128, aes-256-cfb128

- aes-128-ofb, aes-192-ofb, aes-256-ofb

- aes-128-gcm, aes-192-gcm, aes-256-gcm

**Syntax**

```sql

decrypt('mode', 'ciphertext', 'key' [, iv, aad])

```

**Arguments**

- `mode` — Decryption mode. [String](https://bytedance.feishu.cn/sql-reference/data-types/string.md#string) .

- `ciphertext` — Encrypted text that needs to be decrypted. [String](https://bytedance.feishu.cn/sql-reference/data-types/string.md#string) .

- `key` — Decryption key. [String](https://bytedance.feishu.cn/sql-reference/data-types/string.md#string) .

- `iv` — Initialization vector. Required for `-gcm` modes, optinal for others. [String](https://bytedance.feishu.cn/sql-reference/data-types/string.md#string) .

- `aad` — Additional authenticated data. Won't decrypt if this value is incorrect. Works only in `-gcm` modes, for others would throw an exception. [String](https://bytedance.feishu.cn/sql-reference/data-types/string.md#string) .

**Returned value**

- Decrypted String. [String](https://bytedance.feishu.cn/sql-reference/data-types/string.md#string) .

**Examples**

Re-using table from [encrypt](https://bytedance.feishu.cn/docs/doccnZ2mgKjAAc5rAc62DusGjq1#encrypt) .

Query:

```sql

SELECT comment, hex(secret) FROM encryption_test;

```

Result:

```plain%20text

┌─comment──────────────┬─hex(secret)──────────────────────────────────┐

│ aes-256-gcm          │ A8A3CCBC6426CFEEB60E4EAE03D3E94204C1B09E0254 │

│ aes-256-gcm with AAD │ A8A3CCBC6426D9A1017A0A932322F1852260A4AD6837 │

└──────────────────────┴──────────────────────────────────────────────┘

┌─comment─────────────────────────────┬─hex(secret)──────────────────────┐

│ aes-256-cfb128 no IV                │ B4972BDC4459                     │

│ aes-256-cfb128 no IV, different key │ 2FF57C092DC9                     │

│ aes-256-cfb128 with IV              │ 5E6CB398F653                     │

│ aes-256-cbc no IV                   │ 1BC0629A92450D9E73A00E7D02CF4142 │

└─────────────────────────────────────┴──────────────────────────────────┘

```

Now let's try to decrypt all that data.

Query:

```sql

SELECT comment, decrypt('aes-256-cfb128', secret, '12345678910121314151617181920212') as plaintext FROM encryption_test

```

Result:

```plain%20text

┌─comment─────────────────────────────┬─plaintext─┐

│ aes-256-cfb128 no IV                │ Secret    │

│ aes-256-cfb128 no IV, different key │ �4�

                                           �         │

│ aes-256-cfb128 with IV              │ ���6�~        │

 │aes-256-cbc no IV                   │ �2*4�h3c�4w��@

└─────────────────────────────────────┴───────────┘

```

Notice how only a portion of the data was properly decrypted, and the rest is gibberish since either `mode` , `key` , or `iv` were different upon encryption.
