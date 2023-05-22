#include <map>
#include <hudi.pb.h>
#include <JNIArrowReader.h>
#include <JNIArrowStream.h>
#include <JNIHelper.h>
#include <JNIMetaClient.h>

std::string makeProperties(const std::map<std::string, std::string> &params)
{
    DB::Protos::HudiMetaClientParams proto_params;
    auto * mutable_proto = proto_params.mutable_properties();
    for (const auto &kv : params)
    {
        auto *proto_kv = mutable_proto->add_properties();
        proto_kv->set_key(kv.first);
        proto_kv->set_value(kv.second);
    }
    return proto_params.SerializeAsString();
}

void runMetaClient()
{
    JNIEnv * env = DB::JNIHelper::instance().getJNIEnv();
    assert(env != nullptr);

    std::map<std::string, std::string> params = {
        {"A", "a"},
        {"B", "b"}
    };

    std::string pb_params = makeProperties(params);
    
    DB::JNIMetaClient client("org/byconity/common/mock/HelloHudiMetaClient", pb_params);
    std::string table_raw = client.getTable("world");

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
    JNIEnv * env = DB::JNIHelper::instance().getJNIEnv();
    assert(env != nullptr);

    int num_batch = 4;
    int batch_size = 10;
    char s[sizeof(num_batch) * 2];
    memcpy(s, &num_batch, sizeof(int));
    memcpy(s + sizeof(int), &batch_size, sizeof(int));
    std::string params(s, sizeof s);

    DB::JNIArrowReader reader("org/byconity/common/mock/InMemoryReaderBuilder", params);
    reader.initStream();
    const auto & schema = reader.getSchema();
    std::cout << schema.n_children << std::endl;

    DB::ArrowArray chunk;
    while (reader.next(chunk))
    {
        std::cout << chunk.length << " rows" << std::endl;
        chunk.release(&chunk);
    }
}

int main(int, char **)
{
    setenv("CLASSPATH", "/data01/cao.liu.l/ck/ByConity/src/JNI/jni/distribution/target/jni-extension_1.0-SNAPSHOT-jar-with-dependencies.jar", 1);

    runMetaClient();
    //runArrowStream();
}
