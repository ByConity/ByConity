#include <Columns/ColumnString.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/SQLBinding/SQLBinding.h>
#include <Interpreters/SQLBinding/SQLBindingUtils.h>
#include <Parsers/formatAST.h>
#include <Common/OptimizedRegularExpression.h>
#include <boost/regex.hpp>

//#include <Functions/Regexps.h>

namespace DB
{
//using Searcher = std::conditional_t<false, VolnitskyCaseInsensitiveUTF8, VolnitskyUTF8>;

//The priority of bindings matching： Session bindings > global bindings; sql bindings > regular expression bindings;
ASTPtr SQLBindingUtils::getASTFromBindings(const char * begin, const char * end, ContextMutablePtr & context)
{
    if (!context->hasSessionContext() || !BindingCacheManager::getSessionBindingCacheManager(context))
        return nullptr;

    const auto * new_begin = begin;
    const auto * new_end = end;

    // get query hash
    UUID query_hash = getQueryHash(new_begin, new_end);

    // normalize query remove meaningless symbols
    String query = getNormalizedQuery(new_begin, new_end);

    // check session sql binding cache
    auto session_binding_cache_manager = BindingCacheManager::getSessionBindingCacheManager(context);
    auto & session_sql_cache = session_binding_cache_manager->getSqlCacheInstance();
    auto sql_binding_ptr = session_sql_cache.get(query_hash);
    if (sql_binding_ptr && sql_binding_ptr->target_ast)
    {
        LOG_INFO(&Poco::Logger::get("SQL Binding"), "Session SQL Binding Hit");
        return sql_binding_ptr->target_ast->clone();
    }

    std::function<bool(std::list<UUID> &, BindingCacheManager::CacheType &)> is_match_re_bindings
        = [&](std::list<UUID> & re_keys, BindingCacheManager::CacheType & re_cache) {
              for (auto it = re_keys.rbegin(); it != re_keys.rend(); ++it)
              {
                  auto session_re_binding_ptr = re_cache.get(*it);
                  if (session_re_binding_ptr && session_re_binding_ptr->settings
                      && isMatchBinding(query.data(), query.data() + query.size(), *session_re_binding_ptr))
                  {
                      InterpreterSetQuery(session_re_binding_ptr->settings->clone(), context).executeForCurrentContext();
                      LOG_INFO(&Poco::Logger::get("SQL Binding"), "Session Regular Expression Binding Hit");
                      return true;
                  }
              }
              return false;
          };

    // check session re binding cache
    auto session_re_keys = session_binding_cache_manager->getReKeys();
    if (is_match_re_bindings(session_re_keys, session_binding_cache_manager->getReCacheInstance()))
        return nullptr;

    if (!context->getGlobalBindingCacheManager())
        return nullptr;

    auto global_binding_cache_manager = context->getGlobalBindingCacheManager();
    // if (current time_stamp - last update time_stamp) > global_bindings_update_time update global bindings cache from catalog
    auto time_stamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if ((time_stamp - global_binding_cache_manager->getTimeStamp()) > static_cast<long>(context->getSettingsRef().global_bindings_update_time))
    {
        try
        {
            BindingCacheManager::updateGlobalBindingsFromCatalog(context);
        }
        catch (...)
        {
            LOG_INFO(&Poco::Logger::get("SQL Binding"), "Update Global BindingsCache Failed");
        }
    }

    // check global sql bindings cache
    auto & global_sql_cache = global_binding_cache_manager->getSqlCacheInstance();
    auto global_sql_binding_ptr = global_sql_cache.get(query_hash);
    if (global_sql_binding_ptr && global_sql_binding_ptr->target_ast)
    {
        LOG_INFO(&Poco::Logger::get("SQL Binding"), "Global SQL Binding Hit");
        return global_sql_binding_ptr->target_ast->clone();
    }

    // check global re bindings cache
    auto global_re_keys = global_binding_cache_manager->getReKeys();
    is_match_re_bindings(global_re_keys, global_binding_cache_manager->getReCacheInstance());
    return nullptr;
}

UUID SQLBindingUtils::getQueryHash(const char * begin, const char * end)
{
    SipHash hash;
    UInt128 key{};
    Lexer lexer(begin, end);

    while (true)
    {
        Token token = lexer.nextToken();

        if (!token.isSignificant())
            continue;

        if (token.isEnd() || token.isError() || token.type == TokenType::Semicolon)
            break;

        hash.update(token.begin, token.size());
    }
    hash.get128(key);
    return UUID(key);
}

UUID SQLBindingUtils::getReExpressionHash(const char * begin, const char * end)
{
    SipHash hash;
    UInt128 key{};
    hash.update(begin, end - begin);
    hash.get128(key);
    return UUID(key);
}

String SQLBindingUtils::getNormalizedQuery(const char * begin, const char * end)
{
    String res;
    Lexer lexer(begin, end);

    while (true)
    {
        Token token = lexer.nextToken();

        if (!token.isSignificant())
        {
            res += ' ';
            continue;
        }

        if (token.isEnd() || token.isError() || token.type == TokenType::Semicolon)
            break;

        res += String(token.begin, token.size());
    }
    return res;
}

// Refer to the logic of the match()
//bool SQLBindingUtils::matchPattern(const char * begin, const char * end, const String & pattern)
//{
//    try
//    {
//        auto regexp = Regexps::get<false, true, false>(pattern);
//        size_t data_size = end - begin;
//        std::string required_substring;
//        bool is_trivial;
//        bool required_substring_is_prefix; /// for `anchored` execution of the regexp.
//        regexp->getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);
//
//        if (required_substring.empty())
//        {
//            if (!regexp->getRE2()) /// An empty regexp. Always matches.
//                return true;
//
//            return regexp->getRE2()->Match(re2_st::StringPiece(begin, data_size), 0, data_size, re2_st::RE2::UNANCHORED, nullptr, 0);
//        }
//        else
//        {
//            const char * pos;
//            Searcher searcher(required_substring.data(), required_substring.size(), data_size);
//            if (end != (pos = searcher.search(begin, data_size)))
//            {
//                if (is_trivial)
//                    return true;
//                else
//                {
//                    if (required_substring_is_prefix)
//                    {
//                        return regexp->getRE2()->Match(
//                            re2_st::StringPiece(begin, data_size), pos - begin, data_size, re2_st::RE2::UNANCHORED, nullptr, 0);
//                    }
//                    else
//                        return regexp->getRE2()->Match(
//                            re2_st::StringPiece(begin, data_size), 0, data_size, re2_st::RE2::UNANCHORED, nullptr, 0);
//                }
//            }
//        }
//    }
//    catch (...)
//    {
//        return false;
//    }
//    return false;
//}

bool SQLBindingUtils::isMatchBinding(const char * begin, const char *, SQLBindingObject & binding)
{
    try
    {
        auto re = binding.re;
        if (boost::regex_match(begin, *re))
        {
            return true;
        }
    }
    catch (boost::wrapexcept<boost::regex_error> &)
    {
        LOG_ERROR(&Poco::Logger::get("SQL Binding"), "regex_match error");
        return false;
    }

    return false;
}

String SQLBindingUtils::getASTStr(const ASTPtr & ast)
{
    WriteBufferFromOwnString stream;
    formatAST(*ast, stream, false, true);
    return stream.str();
}

String SQLBindingUtils::getShowBindingsHeader(size_t row_number)
{
    return "***************************" + std::to_string(row_number) + ". row ***************************\n";
}

}
