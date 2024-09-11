#include <filesystem>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <Poco/URI.h>
#include <Common/FilePathMatcher.h>
#include <Common/parseGlobs.h>

namespace DB
{

/**
 * @brief Recursive directory listing with matched paths as a result.
 */
Strings FilePathMatcher::regexMatchFiles(const String & path_for_ls, const String & for_match)
{
    const size_t first_glob = for_match.find_first_of("*?{");

    const size_t end_of_path_without_globs = for_match.substr(0, first_glob).rfind('/');
    const String suffix_with_globs = for_match.substr(end_of_path_without_globs); /// begin with '/'
    String prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs); /// ends with '/'

    const size_t next_slash = suffix_with_globs.find('/', 1);
    re2::RE2 matcher(makeRegexpPatternFromGlobs(suffix_with_globs.substr(0, next_slash)));

    Strings result;
    FileInfos file_infos = getFileInfos(prefix_without_globs);
    for (const FileInfo & file_info : file_infos)
    {
        const size_t last_slash = file_info.file_path.rfind('/');
        const String file_name = file_info.file_path.substr(last_slash);
        const bool looking_for_directory = next_slash != std::string::npos;
        /// Condition with type of current file_info means what kind of path is it in current iteration of ls
        if (!file_info.is_directory && !looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                result.push_back(getSchemeAndPrefix() + file_info.file_path);
            }
        }
        else if (file_info.is_directory && looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                Strings result_part
                    = regexMatchFiles(std::filesystem::path(file_info.file_path) / "", suffix_with_globs.substr(next_slash));
                /// Recursion depth is limited by pattern. '*' works only for depth = 1, for depth = 2 pattern path is '*/*'. So we do not need additional check.
                std::move(result_part.begin(), result_part.end(), std::back_inserter(result));
            }
        }
    }

    return result;
}

String FilePathMatcher::removeSchemeAndPrefix(const String & full_path)
{
    String match_path = full_path;
    // remove scheme from path
    Poco::URI uri(full_path);
    // If there is a '?', substring after '?' will be recognized as a query
    if (!uri.getQuery().empty())
        match_path = uri.getPathAndQuery();
    else
        match_path = uri.getPath();

    return match_path;
}
}
