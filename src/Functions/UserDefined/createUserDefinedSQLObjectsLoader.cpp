#include <Functions/UserDefined/createUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsLoader.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

std::unique_ptr<IUserDefinedSQLObjectsLoader> createUserDefinedSQLObjectsLoader(const ContextMutablePtr & global_context)
{
    const auto & config = global_context->getConfigRef();
    String default_path = fs::path{global_context->getPath()} / "user_defined/";
    String path = config.getString("user_defined_path", default_path);
    return std::make_unique<UserDefinedSQLObjectsLoader>(global_context, path);
}

}
