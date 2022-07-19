#pragma once

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <Storages/registerStorages.h>

inline void tryRegisterFunctions()
{
    static struct Register { Register() { DB::registerFunctions(); } } registered;
}

inline void tryRegisterFormats()
{
    static struct Register { Register() { DB::registerFormats(); } } registered;
}

inline void tryRegisterStorages()
{
    static struct Register { Register() { DB::registerStorages(); } } registered;
}

inline void tryRegisterAggregateFunctions()
{
    static struct Register { Register() { DB::registerAggregateFunctions(); } } registered;
}
