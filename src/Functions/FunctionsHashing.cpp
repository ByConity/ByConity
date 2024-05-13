/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include "FunctionsHashing.h"

#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(Hashing)
{
#if USE_SSL
    factory.registerFunction<FunctionMD4>();
    factory.registerFunction<FunctionHalfMD5>();
    factory.registerFunction<FunctionHalfMD5>("halfMD5V2");
    factory.registerFunction<FunctionMD5>();
    factory.registerFunction<FunctionSHA1>();
    factory.registerFunction<FunctionSHA224>();
    factory.registerFunction<FunctionSHA256>();
    factory.registerFunction<FunctionSHA384>();
    factory.registerFunction<FunctionSHA512>();
#endif
    factory.registerFunction<FunctionSipHash64>();
    factory.registerFunction<FunctionSipHash64V2>("sipHash64V2");
    factory.registerFunction<FunctionSipHash64Keyed>();
    factory.registerFunction<FunctionSipHash128>();
    factory.registerFunction<FunctionSipHash128Keyed>();
    factory.registerFunction<FunctionSipHash128Reference>({
        "Like [sipHash128](#hash_functions-siphash128) but implements the 128-bit algorithm from the original authors of SipHash.",
        Documentation::Examples{{"hash", "SELECT hex(sipHash128Reference('foo', '\\x01', 3))"}},
        Documentation::Categories{"Hash"}
    });
    factory.registerFunction<FunctionSipHash128ReferenceKeyed>({
        "Same as [sipHash128Reference](#hash_functions-siphash128reference) but additionally takes an explicit key argument instead of using a fixed key.",
        Documentation::Examples{{"hash", "SELECT hex(sipHash128ReferenceKeyed((506097522914230528, 1084818905618843912),'foo', '\\x01', 3));"}},
        Documentation::Categories{"Hash"}
    });
    factory.registerFunction<FunctionCityHash64>();
    factory.registerFunction<FunctionCityHash64V2>("cityHash64V2");
    factory.registerFunction<FunctionHiveCityHash64>();
    factory.registerFunction<FunctionHiveCityHash64V2>("hiveCityHash64V2");
    factory.registerFunction<FunctionFarmFingerprint64>();
    factory.registerFunction<FunctionFarmFingerprint64V2>("farmFingerprint64V2");
    factory.registerFunction<FunctionFarmHash64>();
    factory.registerFunction<FunctionFarmHash64V2>("farmHash64V2");
    factory.registerFunction<FunctionMetroHash64>();
    factory.registerFunction<FunctionMetroHash64V2>("metroHash64V2");
    factory.registerFunction<FunctionIntHash32>();
    factory.registerFunction<FunctionIntHash64>();
    factory.registerFunction<FunctionURLHash>();
    factory.registerFunction<FunctionJavaHash>();
    factory.registerFunction<FunctionJavaHashUTF16LE>();
    factory.registerFunction<FunctionHiveHash>();
    factory.registerFunction<FunctionFNV1aHash>();
    factory.registerFunction<FunctionFlinkFieldHash>();

    factory.registerFunction<FunctionJavaHashV2>("javaHashV2");
    factory.registerFunction<FunctionJavaHashUTF16LEV2>("javaHashUTF16LEV2");
    factory.registerFunction<FunctionHiveHashV2>("hiveHashV2");

#if !defined(ARCADIA_BUILD)
    factory.registerFunction<FunctionMurmurHash2_32WithSeed>();
    factory.registerFunction<FunctionMurmurHash2_64WithSeed>();
    factory.registerFunction<FunctionMurmurHash3_32WithSeed>();
    factory.registerFunction<FunctionMurmurHash3_64WithSeed>();
    factory.registerFunction<FunctionMurmurHash3_128WithSeed>();

    factory.registerFunction<FunctionMurmurHash2_32V2>("murmurHash2_32V2");
    factory.registerFunction<FunctionMurmurHash2_64V2>("murmurHash2_64V2");
    factory.registerFunction<FunctionMurmurHash3_32V2>("murmurHash3_32V2");
    factory.registerFunction<FunctionMurmurHash3_64V2>("murmurHash3_64V2");
    factory.registerFunction<FunctionGccMurmurHashV2>("gccMurmurHashV2");
    factory.registerFunction<FunctionMurmurHash2_32WithSeedV2>("murmurHash2_32WithSeedV2");
    factory.registerFunction<FunctionMurmurHash2_64WithSeedV2>("murmurHash2_64WithSeedV2");
    factory.registerFunction<FunctionMurmurHash3_32WithSeedV2>("murmurHash3_32WithSeedV2");
    factory.registerFunction<FunctionMurmurHash3_64WithSeedV2>("murmurHash3_64WithSeedV2");

    factory.registerFunction<FunctionSparkHashSimple>();
#endif

    factory.registerFunction<FunctionXxHash32>();
    factory.registerFunction<FunctionXxHash64>();
    factory.registerFunction<FunctionXXH3>(
        {
            "Calculates value of XXH3 64-bit hash function. Refer to https://github.com/Cyan4973/xxHash for detailed documentation.",
            Documentation::Examples{{"hash", "SELECT xxh3('ClickHouse')"}},
            Documentation::Categories{"Hash"}
        },
        FunctionFactory::CaseSensitive);

    factory.registerFunction<FunctionXxHash32V2>("xxHash32V2");
    factory.registerFunction<FunctionXxHash64V2>("xxHash64V2");

    factory.registerFunction<FunctionWyHash64>();

    factory.registerFunction<FunctionBLAKE3>(
    {
        R"(
Calculates BLAKE3 hash string and returns the resulting set of bytes as FixedString.
This cryptographic hash-function is integrated into ClickHouse with BLAKE3 Rust library.
The function is rather fast and shows approximately two times faster performance compared to SHA-2, while generating hashes of the same length as SHA-256.
It returns a BLAKE3 hash as a byte array with type FixedString(32).
)",
        Documentation::Examples{
            {"hash", "SELECT hex(BLAKE3('ABC'))"}},
        Documentation::Categories{"Hash"}
    });

}
}
