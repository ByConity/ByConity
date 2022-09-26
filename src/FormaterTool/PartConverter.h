#pragma once
#include <FormaterTool/PartToolkitBase.h>
#include <Poco/Logger.h>

namespace DB
{

class PartConverter : public PartToolkitBase
{
public:
    PartConverter(const ASTPtr & query_ptr_, ContextMutablePtr context_);
    ~PartConverter() override;
    void execute() override;

private:
    Poco::Logger * log = &Poco::Logger::get("PartConverter");
    String source_path;
    String target_path;
    String data_format;
    String working_path;
};

}
