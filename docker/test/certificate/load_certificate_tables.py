import argparse
import os
import subprocess


def insert_words_in_string(string, before, words):
    nPos = string.find(before) + len(before)
    string = list(string)
    string.insert(nPos + 1, words + ' ')
    string = "".join(string)

    return string


def main(args):

    csv_dir = args.suite_path + '/tables_info'

    csv_file_list = []
    import_cmd_list = []
    for cur_dir, sub_dir, included_file in os.walk(csv_dir):
        for item in included_file:
            if item.endswith('.sql'):
                csv_file_list.append(csv_dir + '/' + item)

    for csv_file_path in csv_file_list:
        with open(csv_file_path, 'r') as f:
            cmd_list = []
            queries = f.readlines()

        table_name_db_name = ((queries[1]).replace('DROP TABLE IF EXISTS ', '').strip())[:-1]

        for idx, item in enumerate(queries):
            queries[idx] = item.strip('\n')

        queries[0] = (queries[0])[:-1]
        queries[1] = (queries[1])[:-1]
        queries[2] = (queries[2])[:-1].replace('`', '\`').replace('[','').replace(']','')
        queries[3] = f"INSERT INTO {table_name_db_name} FORMAT CSV SETTINGS input_format_skip_unknown_fields = 1"

        cmd_list.append(f'{args.client} --query="{queries[0]}"')
        cmd_list.append(f'{args.client} --query="{queries[1]}"')
        cmd_list.append(f'{args.client} --query="{queries[2]}"')
        import_cmd_list.append(f'cat {csv_file_path.replace(".sql", ".csv")} | {args.client} --max_memory_usage=20000000000000  --query="{queries[3]}"')

        for item in cmd_list:
            return_code = os.system(item)
            if return_code != 0:
                print('Failed sql is:', item)
                raise Exception("error !")
        print('Table:', table_name_db_name, 'created')

    # TODO: import in parallel
    procs = []
    for item in import_cmd_list:
        return_code = os.system(item)
        if return_code != 0:
            print('Failed sql is:', item)
            raise f"error !"
    print('Data for suite:', args.suite_path, ' has been loaded')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="load csv to table")
    if "TENANT_ID" in os.environ:
        parser.add_argument('--client', default='clickhouse-client --tenant_id=' + os.environ['TENANT_ID'], help="the cnch client path")
    else:
        parser.add_argument('--client', default='clickhouse-client', help="the cnch client path")
    parser.add_argument('--suite-path', required=True, help="the suite path")

    args = parser.parse_args()

    main(args)
