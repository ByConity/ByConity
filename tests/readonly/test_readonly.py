import argparse
import hashlib
import logging
import os

import pytest
import sqlparse
from clickhouse_driver import Client, errors

skip_case_list = (
    'udf', ## there is a bug for udf: drop function if exists swallow the readonly exception.
    'gis', ## Unknown function ST_AsText

    ## below cases have been checked and driver behavior is strange, skip them for now
    '00999_join_not_nullable_types',
    '00525_aggregate_functions_of_nullable_that_return_non_nullable',
    '52016_mysql_array',
    '10026_test_func_retention2',

    ## unstable case, ok to skip
    '00696_system_columns_limit',

    ## nullable(map) maybe not supported in clickhouse_driver
    '03034_nullable_map',
    '52018_mysql_map',
)

skip_keyword_list = (
    "SKIP_IF_READONLY_CI",
    "SYSTEM START",
    "SYSTEM STOP",
    "SYSTEM SYNC REPAIR TASK",
    "SYSTEM SYNC DEDUP WORKER",
    "SYSTEM DROP CNCH PART CACHE",
    "SERVERERROR",
    "CLIENTERROR",
    "OPTIMIZE TABLE",
    "SLEEPEACHROW"
)

skip_error = (
    "No available service for data.cnch.daemon_manager",
    "Error on connecting daemon-manager",

    "There's no column",
    "Missing columns",
    "not found",
    "already exists",
    "doesn't exist",

    "No available active explicit transaction",
    "No available active transaction",

    "bitmapToArrayWithDecode",
    "DecodeNonBitEngineColumn",
    "Function getMapKeys must apply to ByteMap but given KV map",
)

allowed_error_code = (
    36,
    43,
    47,
    48,
    50,
    53,
    60,
    76,
    80,
    125,
    158,
    164,
    241,
    277,
    307,
    497,
    511,
    516,
    519,
    3011,
    5046,
    99999
)

allowed_statement = (
    "SELECT",
    "EXPLAIN",
    "SHOW",
    "WITH",
    "DESCRIBE",
    "DESC",
    "SYSTEM FLUSH LOGS;",
    "SYSTEM DROP QUERY CACHE;",
    "SYSTEM REMOVE MERGES",
    "SYSTEM SYNC DEDUP WORKER",
    "SYSTEM SYNC REPAIR TASK",
    "SET",
    "USE",
    "SYSTEM DROP MEMORY DICT CACHE",
    "SYSTEM SUSPEND FOR",
    "SYSTEM DROP MARK CACHE",
    "EXISTS",
    "SYSTEM GC",
    "CHECK",
    "IN CACHE",
    "CREATE STATS",
    "DROP STATS",
    "ALTER DISK CACHE",
)


def prepare_logger():
    logger = logging.getLogger('TEST')
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler('test_output/test_readonly.log')
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def parse_arguments():
    parser = argparse.ArgumentParser()

    host = os.environ.get('HOST', '127.0.0.1')
    port = os.environ.get('PORT', '9000')
    mode = os.environ.get('MODE', '1')
    case = os.environ.get('CASE', '')
    cases = os.environ.get('CASE_DIR', './queries/4_cnch_stateless/')

    parser.add_argument('--host', type=str, help='target host ipv4/ipv6', default=host)
    parser.add_argument('--port', type=str, help='target http port', default=port)
    parser.add_argument('--dir', type=str, help='readonly cases dir', default=cases)
    parser.add_argument('-c', type=int, help='pytest concurrency', default=8)
    parser.add_argument('--mode', type=str, help='execution mode, 1 is run DDL, 2 is run all', default=mode)
    parser.add_argument('--case', type=str, help='cases to run', default=case)

    return parser.parse_args()


LOG = prepare_logger()
args = parse_arguments()


def collect_cases():
    dir = args.dir

    sql_test_cases = []
    sh_test_cases = []

    cases = args.case.split(',')
    for root, _, files in os.walk(dir):
        for file in files:
            case_name = os.path.splitext(file)[0]
            if file.endswith('.sql'):
                if not args.case:
                    sql_test_cases.append(case_name)
                    continue
                for case in cases:
                    if case_name == case or (case and case in case_name):
                        sql_test_cases.append(case_name)
                        break
            elif file.endswith('.sh'):
                sh_test_cases.append(case_name)
    return sorted(sql_test_cases, reverse=True)


def recover_context(client, context):
    if context:
        base_sql = "SET "
        settings = []
        for k, v in context.items():
            try:
                v = int(v)
                settings.append(f"{k}={v}")
            except:
                settings.append(f"{k}='{v}'")
        sql = base_sql + ",".join(settings)
        LOG.debug(f"Recovery context: {sql}")

        try:
            client.execute(sql)
        except Exception as e:
            LOG.error(e.message.splitlines()[0])


def query_runner(query, client, context={}):
    query = query.strip()
    LOG.debug(f"QUERY: {query}, DATABASE: {client.connection.database}")
    try:
        client.execute(query)
        code = 0
    except errors.ServerException as e:
        code = e.code
        exception = e.message.splitlines()[0]
        if ("Cannot" in exception and "in readonly mode" in exception) or ("is not writable" in exception):
            code = 164
            LOG.debug(f"Expected exception: CODE {code}: {exception}")
        else:
            for error in skip_error:
                if error in exception:
                    LOG.debug(f"Meet expected server exception: CODE {code}: {exception}")
                    code = 99999
                    recover_context(client,
                                   context)  ## there is a bug in clickhouse_driver, it will lost the context info when execute meets a error
                    return code
            LOG.error(f"Meet unexpected server exception: CODE {code}: {exception}")
    except UnicodeDecodeError as e:
        code = 99999
        LOG.debug(f"Meet expected driver exception: {e}")
    except errors.UnknownTypeError as e:
        code = 99999
        LOG.debug(f"Meet expected driver exception: {e}")
    except (ValueError, KeyError) as e:
        code = 99999
        LOG.debug(f"Meet expected driver exception: {e}")
    recover_context(client, context)
    return code


def database_maker(case_name, client):
    database_hash = hashlib.md5(case_name.encode())
    database_name = 'readonly_' + database_hash.hexdigest()

    if args.mode == "1":
        drop_database_query = f'DROP DATABASE IF EXISTS {database_name}'
        create_database_query = f'CREATE DATABASE {database_name}'
        try:
            database = 'default'
            res = query_runner(drop_database_query, client)
            res = query_runner(create_database_query, client)
        except Exception as e:
            LOG.error(f'Generate database {database_name} fail, reason: {e}')
            database_name = ''

    return database_name


def mode_runner(query, client, context):
    temp_query = query.strip().strip(";").upper()
    if args.mode == "1" and (temp_query.startswith("CREATE")
                             or temp_query.startswith("INSERT")
                             or temp_query.startswith("ALTER")
                             or temp_query.startswith("USE")
                             or temp_query.startswith("SET")
                             or temp_query.startswith("GRANT")):
        res = query_runner(query, client, context)
    if args.mode == "2":
        res = query_runner(query, client, context)
        accepted_code = list(allowed_error_code)
        for allow in allowed_statement:
            while temp_query.startswith('('):
                temp_query = temp_query[1:]
            if temp_query.startswith(allow) or temp_query.endswith(allow):
                # LOG.debug(f"Query suits allowed_statement rule, start with: {allow}")
                accepted_code.append(0)
                break
        if res not in accepted_code:
            LOG.error(f"Meet unaccepted exception: CODE {res}")
            LOG.error("ERROR")
        assert res in accepted_code


def get_sql(case_path):
    sql_statements = []

    with open(case_path, 'r') as f:
        try:
            content = f.read()
        except Exception as e:
            LOG.warning(f'{case_path} is not readable since: {e}')
            return sql_statements

    # format all content so that it will easy to split the sql
    content = sqlparse.format(content).splitlines()

    i, length = 0, len(content)
    temp_sql = ""

    while i < length:
        line = content[i]
        strip_comment_line = sqlparse.format(line, strip_comments=True)

        if not strip_comment_line:
            i += 1
            continue

        flag = True
        for keyword in skip_keyword_list:
            if keyword in line.upper():
                temp_sql = ""
                flag = False
                break
        if not flag:
            i += 1
            continue

        temp_sql += strip_comment_line + "\n"
        temp_sql = sqlparse.format(temp_sql, strip_comments=True)
        if temp_sql == ";":
            temp_sql = ""
        if temp_sql.endswith(";") and not temp_sql.startswith("/*"):
            sql_statements.append(temp_sql.strip('\n'))
            temp_sql = ""
        i += 1

    return sql_statements


def init_client(database):
    host = args.host
    port = args.port
    user = 'default'
    password = ''

    client = Client(host=host, port=port, user=user, password=password, database=database)

    return client


def kill_client(client):
    try:
        client.disconnect()
    except Exception as e:
        LOG.error(f"Can't disconnect since {e}")


def split_settings(s):
    settings = {}
    s = s.strip()[3:]
    raw_settings = s.split(',')
    for raw_setting in raw_settings:
        raw_setting = raw_setting.strip()
        raw_k, raw_v = raw_setting.split("=")
        k, v = normalize(raw_k), normalize(raw_v)
        settings.update({k: v})
    return settings


def normalize(s):
    return s.strip().replace(";", '').replace("'", '')


@pytest.mark.parametrize("case_name", collect_cases())
def test(case_name):
    # skip typical cases
    for name in skip_case_list:
        if name in case_name:
            pytest.skip(f'{case_name} will be skipped since skip rule [{name}]')

    # get sql statements
    case_path = os.path.join(args.dir, case_name + '.sql')
    sql_statements = get_sql(case_path)

    # create database
    client = init_client("")
    last_database_name = database_maker(case_name, client)

    assert last_database_name != ""

    client.execute(f"USE {last_database_name}")

    # run all query
    context = {}
    for sql_statement in sql_statements:
        if sql_statement.startswith('set ') or sql_statement.startswith('SET '):
            context.update(split_settings(sql_statement))
        mode_runner(sql_statement, client, context)

    # disconnect
    kill_client(client)


def check_health():
    database = "default"
    if args.mode == "1":
        drop_query = 'DROP DATABASE if exists test;'
        create_query = 'CREATE DATABASE test;'
        client = init_client(database)

        res = query_runner(drop_query, client)
        assert res == 0
        res = query_runner(create_query, client)
        assert res == 0
    else:
        query = "select 1"
        client = init_client(database)

        res = query_runner(query, client)
        print(res)
        assert res == 0


if __name__ == "__main__":
    check_health()
    pytest_config = [
        "test_readonly.py",
        "-v",
        "-x",
        f"-n={args.c}",
        "--disable-warnings",
        "--color=yes",
        "--noconftest",
        "--timeout=300",
        "--durations=30"
    ]

    res = pytest.main(pytest_config)

    if not res==0:
        exit(1)
