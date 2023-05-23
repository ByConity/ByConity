#include <map>
#include <hudi.pb.h>
#include <JNIArrowReader.h>
#include <JNIArrowStream.h>
#include <JNIHelper.h>
#include <JNIMetaClient.h>

void makeProperties(DB::Protos::Properties * prop, const std::map<std::string, std::string> &params)
{
    for (const auto &kv : params)
    {
        auto *proto_kv = prop->add_properties();
        proto_kv->set_key(kv.first);
        proto_kv->set_value(kv.second);
    }
}

void runMetaClient()
{
    std::cout << "run meta client" << std::endl;
    JNIEnv * env = DB::JNIHelper::instance().getJNIEnv();
    assert(env != nullptr);

    std::map<std::string, std::string> params = {
        {"A", "a"},
        {"B", "b"}
    };

    DB::Protos::HudiMetaClientParams req;
    makeProperties(req.mutable_properties(), params);
    
    DB::JNIMetaClient client("org/byconity/common/mock/HelloHudiMetaClient", req.SerializeAsString());
    std::string table_raw = client.getTable();

    DB::Protos::HudiTable table;
    table.ParseFromString(table_raw);
    std::cout << table.hive_db_name() << std::endl;
    std::cout << table.hive_table_name() << std::endl;

    for (const auto &kv : table.properties().properties())
    {
        std::cout << kv.key() << ' ' << kv.value() << std::endl;
    }
}

void runArrowStream()
{
    std::cout << "run arrow stream" << std::endl;
    JNIEnv * env = DB::JNIHelper::instance().getJNIEnv();
    assert(env != nullptr);

    std::map<std::string, std::string> params;
    params["num_batch"] = "4";
    params["batch_size"] = "10";
    DB::Protos::Properties req;
    makeProperties(&req, params);

    DB::JNIArrowReader reader("org/byconity/common/mock/InMemoryReaderBuilder", req.SerializeAsString());
    reader.initStream();
    auto schema = reader.getSchema();
    std::cout << schema.n_children << std::endl;

    ArrowArray chunk;
    while (reader.next(chunk))
    {
        std::cout << chunk.length << " rows" << std::endl;
        chunk.release(&chunk);
    }
}

int main(int, char **)
{
    const char * env = getenv("CLASSPATH");
    if (env == nullptr)
    {
        std::cerr << "CLASSPATH is not set. CLASSPATH should be set to the path of 'jni-extension_1.0-SNAPSHOT-jar-with-dependencies.jar'" << std::endl;
        return 1;
    }

    runMetaClient();
    runArrowStream();
}
