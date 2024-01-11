# This script is responsible for check the GLIBC compatibility.
# Please install `packaging` before run this script.
import argparse
import logging
import subprocess
import uuid
import os

from packaging.version import Version, parse
import re


MAX_GLIBC_VERSION = "2.4"


def run_commands(commands):
    state = "success"
    for run_command in commands:
        try:
            logging.info("Running command %s", run_command)
            subprocess.check_call(run_command, shell=True)
        except subprocess.CalledProcessError as ex:
            logging.info("Exception calling command %s", ex)
            state = "failure"

    logging.info("Result: %s", state)


def generate_commands_for_glibc_check(binary_path, tmp_result_dir):
    ret = [
        f"mkdir -p {tmp_result_dir}",
        f"objdump -T {binary_path} | grep GLIBC_ > {tmp_result_dir}/glibc_syms.txt",
    ]
    return ret


def process_glibc_check(log_path, max_glibc_version=MAX_GLIBC_VERSION):
    max_glibc_version = parse(max_glibc_version)
    with open(log_path, "r") as log:
        for line in log:
            version_numbers = re.findall(r"GLIBC_([\d.]+)", line)
            for version in version_numbers:
                version = version.strip("()")
                if parse(version) > max_glibc_version:
                    return False, line
    return True, ""


def parse_args():
    parser = argparse.ArgumentParser("Check compatibility with old distributions")
    parser.add_argument("--binary", required=True)
    parser.add_argument("--check-glibc", action="store_true")
    parser.add_argument("--tmp-result-dir")

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    tmp_result_dir = args.tmp_result_dir

    if not os.path.exists(args.binary):
        logging.error(f"file {args.binary} doesn't exist.")
        exit(1)

    if not tmp_result_dir:
        tmp_result_dir = "/tmp/" + str(uuid.uuid4())
    logging.info(f"Tmp result dir: {tmp_result_dir}/")

    commands = generate_commands_for_glibc_check(args.binary, tmp_result_dir)

    run_commands(commands)

    res, msg = process_glibc_check(tmp_result_dir + "/glibc_syms.txt")

    if not res:
        logging.error("GLIBC check failed with: %s", msg)
        exit(1)
    else:
        logging.info("GLIBC check passed.")


if __name__ == "__main__":
    main()
