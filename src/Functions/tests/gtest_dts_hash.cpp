#include <gtest/gtest.h>

#include <Functions/dtsCityHash.h>
#include <Functions/hiveIntHash.h>
#include "BigInteger.hh"
#include "BigIntegerUtils.hh"

/// Test dtsCityHash
static void test(std::string s, int64_t expected, uint64_t expected_us)
{
    int64_t r = DB::DTSCityHash::cityHash64(s.data(), 0, s.length());
    EXPECT_EQ(r, expected);
    EXPECT_EQ(DB::DTSCityHash::asUInt64(r), expected_us);
}

TEST(DTSCityHash, Test0to4)
{
    test("abc", 4220206313085259313L, 4220206313085259313UL);
    test("111", 5518486022181526874L, 5518486022181526874UL);
    test("-111", 5271023622228185005L, 5271023622228185005UL);
}

TEST(DTSCityHash, Test5to7)
{
    test("abcabc", -2180749251667803241L, 16265994822041748375UL);
}

TEST(DTSCityHash, Test8to16)
{
    test("abcabcabc", -363239307316136316L, 18083504766393415300UL);
    test("e43970221139c", 651451745298338306L, 651451745298338306UL);
}

TEST(DTSCityHash, Test17to32)
{
    test("abcabcabcabcabcabcabcabc", -8992032506223809229L, 9454711567485742387UL);
}

TEST(DTSCityHash, Test33to64)
{
    test("abcabcabcabcabcabcabcabcabcabcabcabc", -8163064513280341296L, 10283679560429210320UL);
}

TEST(DTSCityHash, Test64Plus)
{
    test("abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc", -822948244188030202L, 17623795829521521414UL);
}

TEST(DTSCityHash, TestCN)
{
    test("中国人", -4667871355360362275L, 13778872718349189341UL);
    test("中国人中国人中国人中国人中国人中国人中国人中国人中国人中国人", -5394058890835601704L, 13052685182873949912UL);
}

/// Test hiveIntHash
static void test_int(BigInteger x, BigInteger expected)
{
    BigInteger r = DB::HiveIntHash::intHash64(x);
    EXPECT_EQ(r, expected);
}

TEST(HiveIntHash, TestInt)
{
    test_int(stringToBigInteger("111"), stringToBigInteger("28960595049120298284007597794249568656608"));
    test_int(stringToBigInteger("-111"), stringToBigInteger("28699688790393188402562795085604323520147"));
}

TEST(HiveIntHash, TestLong)
{
    test_int(stringToBigInteger("4294967297"), stringToBigInteger("1120583861364653015936649848396047605431838178733"));
    test_int(stringToBigInteger("-4294967297"), stringToBigInteger("1120583861278725678966405905726831886655789209759"));
}

TEST(HiveIntHash, TestBigInt)
{
    test_int(stringToBigInteger("340282366920938463463374607431768211457"), stringToBigInteger("88781800262427497179229347262209927076731686175940560623852567004281829509932"));
    test_int(stringToBigInteger("-340282366920938463463374607431768211457"), stringToBigInteger("88781800262427497179229347262209927076101144782869356141092329336787357925376"));
}
