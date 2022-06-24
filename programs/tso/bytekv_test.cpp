#include <TSO/TSOByteKVImpl.h>
#include <iostream>
#include <string>

/// test put get and delete;
bool testPutGetDel(DB::TSO::TSOByteKVImpl &);

using String = std::string;

void logInfo(const String & info) {std::cout << info << std::endl;}
void logError(const String & error) {std::cerr << error << std::endl;}

void reportTestResult(const String & test, const bool passed)
{
    if (passed)
        logInfo(test + " test succeed!");
    else
        logInfo(test + " test falied!");
}

int main()
{

    DB::TSO::TSOByteKVImpl tso_kv("toutiao.bytekv.proxy.service.lq",
        "user_test", "olap_cnch_test", "cnch_tso", "tso");

    bool testPutGetDel_succeed = testPutGetDel(tso_kv);
    reportTestResult("Put/Get/Del", testPutGetDel_succeed);
}

bool testPutGetDel(DB::TSO::TSOByteKVImpl & tso_kv)
{
    bool passed = true;
    String test_value = "11111";

    tso_kv.put(test_value);
    String get_res;
    tso_kv.get(get_res);
    if (get_res != test_value)
    {
        logError("Failed to get inserted value from bytekv.");
        passed =  false;
    }

    tso_kv.clean();
    tso_kv.get(get_res);

    if (get_res != "")
    {
        logError("Failed to delete value from bytekv.");
        passed = false;
    }
    return passed;
}
