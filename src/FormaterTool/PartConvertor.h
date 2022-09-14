#pragma once
#include <FormaterTool/PartToolkitBase.h>
#include <Poco/Logger.h>

namespace DB
{

class PartConvertor : public PartToolkitBase
{
public:
    PartConvertor(const ASTPtr & query_ptr_, ContextMutablePtr context_);
    void execute() override;

private:
    Poco::Logger * log = &Poco::Logger::get("PartConvertor");
    String source_path;
    String target_path;
    String data_format;
    String working_path;
};

}
