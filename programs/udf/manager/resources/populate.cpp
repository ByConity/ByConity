#if defined(__has_warning) && __has_warning("-Wreserved-identifier")
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif
#include <cstring>

#include <fcntl.h>
#include <sys/stat.h>

#include "populate.h"
#include "resources.h"

std::optional<std::string> populate_binaries(char *path)
{
    struct bin_lists {
        const char *name;
        const char *start;
        size_t size;
    };

    static const struct bin_lists bins[] = {
        {"iudf.py", &_binary_iudf_py_start, _binary_iudf_py_size},
        {"overload.py", &_binary_overload_py_start, _binary_overload_py_size},
    };
    if (!path) {
        return "path empty";
    }
    size_t len = strlen(path);

    mkdir(path, 0755);

    path[len++] = '/';
    /* for each binary, save the context */
    for (const auto &bin : bins) {
        FILE *fp;

        strcpy(path + len, bin.name);
        fp = fopen(path, "w");

        if (!fp)
            return std::string("failed to open ") + path;

        if (fwrite(bin.start, bin.size, 1, fp) < 1)
            return std::string("failed to write") + path;

        fclose(fp);
    }
    return std::nullopt;
}

