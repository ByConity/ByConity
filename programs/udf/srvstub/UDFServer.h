#pragma once
#include <memory>
#include <string>
#include <Poco/Util/ServerApplication.h>
#include "ILanguageServer.h"

class UDFImpl;

class UDFServer: public Poco::Util::ServerApplication
{
public:
    explicit UDFServer(ILanguageServer *i) : srv(i) {}
    ~UDFServer() override = default;

protected:
    int main(const std::vector<std::string> &args) override;

private:
    std::shared_ptr<UDFImpl> udf_service;
    ILanguageServer *srv;
};
