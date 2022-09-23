import requests
import logging
import json
import datetime
from requests import HTTPError
import pytz
import os
import sys
import argparse

CUR_DIR = sys.path[0]

# how to get token: https://open.feishu.cn/document/ukTMukTMukTM/ukDNz4SO0MjL5QzM/g#top_anchor
# reference: https://bytedance.feishu.cn/wiki/wikcnVQil2i5yLvSItBT0xt0Qtb#

"""
the best practice is to put the consts below to a consts.py file and import the file.
"""
LARK_OPEN_API = {'get_tenant_token': 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/',
                 'send_text_msg': 'https://open.feishu.cn/open-apis/message/v4/send/',
                 'send_message_card': 'https://open.feishu.cn/open-apis/message/v4/send/'}

# how to get chat_id : https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/chat-id-description
LARK_GROUP_CHAT_ID = {'ch_dev': 'ou_40fa223f06cb79677f0a65ddf474b868',
                      'cnch_debug': 'oc_5dc734075eacec620d12c7431c0fec05',
                      'xh-test': 'oc_bc9c4fc23feeab822dc81cb077b12323'}


def send_lark_api_request(url: str, method: str, **kwargs) -> dict or None:
    ret = requests.request(url=url, method=method, **kwargs)
    if ret.status_code != 200:
        logging.error('Call Api Error, status_code={}, resp={}'.format(ret.status_code, ret.text))
        raise HTTPError('Call Api Error, status_code={}'.format(ret.status_code), response=ret.text)
    resp = ret.json()
    if resp.get('code') == 0:
        return resp
    else:
        logging.error('Call Api Error, request url={}, errorCode={}, msg={}'.format(url, resp['code'], resp['msg']))
        raise Exception('Call Api Error, request url={}, errorCode={}, msg={}'.format(url, resp['code'], resp['msg']))


def get_app_info():
    # get app info from environment
    app_id = os.environ.get('lark_bot_app_id', 'cli_a2afd160f9b85013')
    app_secret = os.environ.get('lark_bot_app_secret', 'MUtiDiOiutJt4X3h3qxdbgYYzMJXYlBb')
    return {'app_id': app_id, 'app_secret': app_secret}


APP_INFO = get_app_info()


def get_tenant_token() -> str:
    app_id = APP_INFO.get('app_id')
    app_secret = APP_INFO.get('app_secret')
    url = LARK_OPEN_API.get('get_tenant_token')
    params = {'app_id': app_id, 'app_secret': app_secret}
    resp = send_lark_api_request(url, 'POST', json=params)
    tenant_token = resp.get('tenant_access_token')
    return tenant_token


TENANT_TOKEN = get_tenant_token()


def send_text_message(tenant_token: str, msg: str, receive_id: str, receive_id_type='email'):
    """
    send text message
    :param tenant_token: tenant_access_token
    :param msg: the message to be sent.
    :param receive_id: receiver id
    :param receive_id_type: type of the receiver_id. open_id/user_id/union_id/email/chat_id.
    :return:
    """
    url = LARK_OPEN_API.get('send_text_msg')
    headers = {'Authorization': 'Bearer ' + tenant_token, 'Content-Type': 'application/json; charset=utf-8'}
    body = {receive_id_type: receive_id, "msg_type": "text", "content": {"text": msg}}
    send_lark_api_request(url, 'POST', headers=headers, json=body)


def send_message_card(tenant_token: str, card: dict, receive_id: str, receive_id_type='email'):
    """
    send message card
    :param tenant_token: tenant_access_token
    :param card: message_card. https://open.feishu.cn/tool/cardbuilder?from=howtoguide use this link to build your card.
    :param receive_id: receiver id
    :param receive_id_type: type of the receiver_id. open_id/user_id/union_id/email/chat_id.
    :return:
    """
    url = LARK_OPEN_API.get('send_message_card')
    headers = {'Authorization': 'Bearer ' + tenant_token, 'Content-Type': 'application/json; charset=utf-8'}
    body = {receive_id_type: receive_id, 'msg_type': 'interactive', 'card': card}
    send_lark_api_request(url, 'POST', headers=headers, json=body)


def send_dump_message_card(data):
    recipient, test_name, date_time, corefile_list, minidump_file_list, mr_link, peek_gdb_info = data

    with open(CUR_DIR + "/lark_template_coredump.json", 'r', encoding='utf-8') as lark_message_card:
        lark_message_card = json.load(lark_message_card)

    # API will validates recipient
    if "@bytedance.com" in recipient:
        content_at = f"<at email={recipient}></at>"
    else:
        content_at = f"***{recipient}***"  # cron job has no recipient

    if corefile_list and minidump_file_list:
        message_title = "  📢 cordedump and minidump detected in MR"
        content_core = f"***corefile***：{str(corefile_list)}\n📄 ***minidump_file***：{str(minidump_file_list)}"

    elif corefile_list:
        message_title = "  📢 cordedump detected in MR"
        content_core = f"***corefile***：{str(corefile_list)}"
    else:
        message_title = "  📢 minidump detected in MR"
        content_core = f"***minidump_file***：{str(minidump_file_list)}"

    # update body
    lark_message_card['header']['title']['content'] = message_title
    lark_message_card['i18n_elements']['zh_cn'][0]['text'][
        'content'] = f"Hi {content_at}, please take action for below files generated from your MR \n{content_core}\n\n{peek_gdb_info}"
    lark_message_card['i18n_elements']['zh_cn'][1]['fields'][0]['text']['content'] = f"**🗂️ Test Name**：{test_name}"
    lark_message_card['i18n_elements']['zh_cn'][1]['fields'][1]['text']['content'] = f"**📅 Datetime**：{date_time}"

    if mr_link:
        lark_message_card['i18n_elements']['zh_cn'][2]['actions'][0]['url'] = mr_link
    else:
        lark_message_card['i18n_elements']['zh_cn'][2]['actions'][0]['text']['content'] = 'Check Cron Jobs'
        lark_message_card['i18n_elements']['zh_cn'][2]['actions'][0]['url'] = "https://code.byted.org/dp/ClickHouse/+/pipelines?trigger=cron"

    print("lark_message_card dict is:", lark_message_card)

    # send to user
    if '@bytedance.com' in recipient:
        send_message_card(tenant_token=TENANT_TOKEN, card=lark_message_card, receive_id=recipient)

    # send to group
    send_message_card(tenant_token=TENANT_TOKEN, card=lark_message_card, receive_id_type='chat_id',receive_id=LARK_GROUP_CHAT_ID['cnch_debug'])


def get_mr_owner():
    ci_event_name = os.environ.get('CI_EVENT_NAME', None)
    if ci_event_name == 'cron':
        return 'CI_cron_job'

    owner = os.environ.get('CI_ACTOR', None)
    if owner:
        return owner + "@bytedance.com"
    else:
        raise Exception('No Onwer found in env!')


def get_mr_test_name():
    test_name = os.environ.get('CI_JOB_NAME', None)
    if test_name:
        return test_name
    else:
        raise Exception('No Test Name found in env!')


def get_corefile_list(base_dir):
    names = os.listdir(base_dir)  # the path of saving core. file
    result = []
    for name in names:
        if name.startswith('core.'):
            result.append(name)
    return result


def get_minidump_file_list(base_dir):
    result = []
    for filepath, dirnames, filenames in os.walk(base_dir):
        for filename in filenames:
            if filename.endswith('.dmp'):
                result.append(os.path.basename(filepath) + '/' + filename)
    return result


def get_mr_link():
    return os.environ.get('CI_EVENT_CHANGE_URL', None)


def peek_gdb_info(base_dir='/shared/'):
    lst = []
    max_number_of_file = 3
    counter = 0
    for filepath, dirnames, filenames in os.walk(base_dir):
        for filename in filenames:
            if filename.endswith('.gdb_info_extracted') and counter <= max_number_of_file:
                with open(filepath + "/" + filename, 'r') as peek_gdb:
                    lst += (peek_gdb.readlines())
                    counter += 1

    string = ''
    if lst:
        for line in lst:
            string += line
        string += '**only limited gdb info in the card, please check full log in CI artifacts.**'

    return string


def get_current_datetime():
    # ci is using moscow time zone, we need to convert it to UTC+8
    return str(datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime("%d-%b-%Y %H:%M:%S"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="lark message bot for CI")
    parser.add_argument("--minidump-path", default='/shared/', help='the path of saving minidump')
    parser.add_argument("--coredump-path", default='/', help='the path of saving coredump')
    args = parser.parse_args()

    corefile_list = get_corefile_list(args.coredump_path)
    minidump_file_list = get_minidump_file_list(args.minidump_path)

    if not corefile_list and not minidump_file_list:
        print('no alert will be sent to user due to no coredump or minidump file')
        exit(0)

    data = get_mr_owner(), get_mr_test_name(), get_current_datetime(), corefile_list, minidump_file_list, get_mr_link(), peek_gdb_info()

    send_dump_message_card(data)
