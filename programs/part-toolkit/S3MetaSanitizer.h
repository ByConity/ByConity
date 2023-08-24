#include <Poco/Util/Application.h>
#include <Core/Types.h>
#include <Catalog/IMetastore.h>
#include <Protos/DataModelHelpers.h>

namespace DB
{

class S3MetaSanitizer: public Poco::Util::Application
{
public:
    S3MetaSanitizer() = default;

    static void sanitizePartMeta(const Protos::S3SanitizerDataModelPart& origin_part,
        Protos::DataModelPart& new_part);

private:
    void defineOptions(Poco::Util::OptionSet& options) override;

    void initialize(Poco::Util::Application& self) override;

    int main(const std::vector<std::string>& args) override;

    void initializeMetastore();
    void sanitizePartsMeta(const std::string& prefix);

    std::string catalog_namespace;
    std::shared_ptr<Catalog::IMetaStore> metastore_ptr;

    Poco::Logger* logger;
};

}
