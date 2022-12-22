#pragma once

#include <Common/Exception.h>

namespace DB
{
class DataTransException : public Exception
{
public:
    DataTransException(const std::string & msg, int code) : Exception(msg, code) { }

    DataTransException * clone() const override { return new DataTransException(*this); }
    void rethrow() const override { throw *this; }

private:
    const char * name() const throw() override { return "DB::DataTransException"; }
    const char * className() const throw() override { return "DB::DataTransException"; }
};

}
