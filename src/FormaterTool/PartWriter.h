#pragma once
#include <FormaterTool/PartToolkitBase.h>
#include <Poco/Logger.h>

namespace DB
{

class PartWriter : public PartToolkitBase
{
public:
    PartWriter(const ASTPtr & query_ptr_, ContextMutablePtr context_);
    void execute() override;

private:
    Poco::Logger * log = &Poco::Logger::get("PartWriter");
    String source_path;
    String data_format;
    String dest_path;
    String working_path;
};

}
