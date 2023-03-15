Allow to build ByConity in Docker for different platforms with different
compilers and build settings. Correctly configured Docker daemon is single dependency.

Usage:
First step, at the root directory of the source code, rm folder `build_docker` if it exist

For example to build a deb package:
```
$ mkdir deb/test_output/
$ ./packager --output-dir deb/test_output/ --package-type deb --docker-image-version 0.1 --ccache_dir=/data01/minh.dao/.ccache_for_docker --version 0.1.1.1
$ ls -l deb/test_output/
```

Build both Debian package and RPM package
```
$ mkdir deb/test_output/
$ ./packager --output-dir deb/test_output/ --package-type deb --docker-image-version 0.1 --ccache_dir=/your/cache/path/.ccache_for_docker --additional-pkgs --version 0.1.1.1
$ ls -l deb/test_output/
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002       7171 Feb 24 09:33 byconity-client-0.1.1.1-amd64.tgz
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002       9038 Feb 24 09:33 byconity-client-0.1.1.1.x86_64.rpm
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002       7772 Feb 24 09:33 byconity-client_0.1.1.1_amd64.deb
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002       7781 Feb 24 09:33 byconity-client_0.1.1.1_x86_64.apk
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002  960963891 Feb 24 09:36 byconity-common-static-0.1.1.1-amd64.tgz
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002 1018306618 Feb 24 09:34 byconity-common-static-0.1.1.1.x86_64.rpm
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002 1018222256 Feb 24 09:33 byconity-common-static_0.1.1.1_amd64.deb
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002 1018317866 Feb 24 09:33 byconity-common-static_0.1.1.1_x86_64.apk
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002       8865 Feb 24 09:36 byconity-daemon-manager-0.1.1.1-amd64.tgz
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      12505 Feb 24 09:36 byconity-daemon-manager-0.1.1.1.x86_64.rpm
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      10036 Feb 24 09:36 byconity-daemon-manager_0.1.1.1_amd64.deb
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      10315 Feb 24 09:36 byconity-daemon-manager_0.1.1.1_x86_64.apk
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      11494 Feb 24 09:36 byconity-server-0.1.1.1-amd64.tgz
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      15441 Feb 24 09:36 byconity-server-0.1.1.1.x86_64.rpm
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      12866 Feb 24 09:36 byconity-server_0.1.1.1_amd64.deb
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      13189 Feb 24 09:36 byconity-server_0.1.1.1_x86_64.apk
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002       8360 Feb 24 09:36 byconity-tso-0.1.1.1-amd64.tgz
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      11860 Feb 24 09:36 byconity-tso-0.1.1.1.x86_64.rpm
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002       9518 Feb 24 09:36 byconity-tso_0.1.1.1_amd64.deb
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002       9788 Feb 24 09:36 byconity-tso_0.1.1.1_x86_64.apk
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      11644 Feb 24 09:36 byconity-worker-0.1.1.1-amd64.tgz
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      15605 Feb 24 09:36 byconity-worker-0.1.1.1.x86_64.rpm
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      16630 Feb 24 09:36 byconity-worker-write-0.1.1.1-amd64.tgz
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      20731 Feb 24 09:36 byconity-worker-write-0.1.1.1.x86_64.rpm
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      18114 Feb 24 09:36 byconity-worker-write_0.1.1.1_amd64.deb
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      18503 Feb 24 09:36 byconity-worker-write_0.1.1.1_x86_64.apk
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      13012 Feb 24 09:36 byconity-worker_0.1.1.1_amd64.deb
Feb 24 09:37:11 -rw-r--r-- 1 1002 1002      13330 Feb 24 09:36 byconity-worker_0.1.1.1_x86_64.apk
Feb 24 09:37:11 -rwxr-xr-x 1 1002 1002 3721272984 Feb 24 09:31 clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-benchmark -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-client -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-compressor -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-copier -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-extract-from-config -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-format -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-git-import -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-keeper -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-keeper-converter -> clickhouse
Feb 24 09:37:11 -rwxr-xr-x 1 1002 1002 2207402976 Feb 24 09:30 clickhouse-library-bridge
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-local -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-meta-inspector -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-obfuscator -> clickhouse
Feb 24 09:37:11 -rwxr-xr-x 1 1002 1002 2214093736 Feb 24 09:30 clickhouse-odbc-bridge
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-part-toolkit -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-server -> clickhouse
Feb 24 09:37:11 lrwxrwxrwx 1 1002 1002         10 Feb 24 09:32 clickhouse-worker -> clickhouse
```

The version of the package is default get from `cmake/autogenerated_versions.txt` file, version of the output package can be changed by using the `--version` parameter. The `cmake/autogenerated_versions.txt` can be updated using the `tests/ci/version_helper.py` script, with syntax
```
tests/ci/version_helper.py --update patch|minor|major 
```


