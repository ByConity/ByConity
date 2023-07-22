#include <alloca.h>
#include <cstdlib>
#include <cstring>

static bool path_in_str(const char *s, const char *path, size_t len)
{
    const char *next;

    if (!s)
        return false;

    while (true) {
        next = strchr(s, ':');

        if (!strncmp(s, path, len)) {
            switch (s[len]) {
            case '\0':
            case ':':
                return true;
            case '/':
                switch (s[len + 1]) {
                case '\0':
                case ':':
                    return true;
                }
            }
        }

        if (!next)
            break;

        s = next + 1;
    }

    return false;
}

static int append_to_env(const char *env, const char *path)
{
    size_t path_len = strlen(path);
    const char *e = getenv(env);
    size_t env_size;
    size_t len;
    char *buf;

    if (path[path_len - 1] == '/')
        path_len--;

    if (path_in_str(e, path, path_len))
        return 0;

    env_size = e ? strlen(e) : 0;
    len = env_size + strlen(path) + 2;

    if (len > 16384)
        return -1;

    buf = static_cast<char *>(alloca(len));
    if (e) {
        strcpy(buf, e);
        buf[env_size] = ':';
        len = env_size + 1;
    } else {
        len = 0;
    }
    strncpy(buf + len, path, path_len);
    buf[len + path_len] = '\0';

    setenv(env, buf, 1);
    return 0;
}

