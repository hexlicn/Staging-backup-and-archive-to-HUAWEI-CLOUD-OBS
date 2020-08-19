#!/usr/bin/python3
# -*- coding: UTF-8 -*-
"""
用于上传本地文件到obs上

"""

import os
import sys
import logging
import logging.handlers
import datetime
import configparser
import subprocess
import time

EXIT_ERROR = 1
TOOL_DIR = os.getcwd()
OBSUTIL_CONFIG = ".obsutilconfig"
SCRIPT_CONFIG = "obsutil_adapter.cfg"
LOG_DIR = "/var/log/huawei/obsutil_adapter"
OBSUTIL_LOG = "obsutil_adapter.log"
OBSUTIL_LOG_FILE = os.path.join(LOG_DIR, OBSUTIL_LOG)
HIDDEN_FILENAME = ".obsutiladapter"

THRESHOLD = 1073741824  # defaultBigfileThreshold = 1G; defaultPartSize = 1G
LOGGING_LEVEL = logging.INFO


def get_config():
    """获取配置
    :return:
    """
    conf = configparser.ConfigParser()
    conf.read(SCRIPT_CONFIG)
    return conf


# pylint: disable=too-many-return-statements
def check_config(conf):
    """检查配置文件各配置项是否正确
    :param conf: 配置项
    :return: 返回False表示有配置项错误，为True表示配置项都正常
    """
    try:
        obs_path = conf.get("obs", "obs_path")
        if obs_path is None or obs_path == "":
            LOG.error("The param obs_path is empty")
            return False
        bucket_name = obs_path.split('/')[0]
        cmd = "./obsutil ls | grep -w %s" % bucket_name
        bucket_check = execute_cmd(cmd)
        if not bucket_check["data"]:
            LOG.error("Bucket name %s not found", bucket_name)
            return False
        retry_times = conf.getint("base", "retry_times")
        if retry_times < 0:
            LOG.error("the param retry_times is incorrect")
            return False
        modified_interval = conf.getint("base", "modified_interval")
        if modified_interval < 0:
            LOG.error("the param modified_interval is incorrect")
            return False
        reserve_time = conf.getint("base", "reserve_time")
        if reserve_time < 0:
            LOG.error("the param reserve_time is incorrect")
            return False
        # support multiple backup folders
        backup_path_list = conf.get("directory", "backup_path").replace(' ', '').split(',')
        backup_archive = conf.get("directory", "backup_archive")
    except configparser.Error as e:
        LOG.error("Some param is wrong, please check: %s", e)
        return False

    if not os.path.exists(backup_archive):
        LOG.warning("The directory %s does not exist, create it", backup_archive)
        os.makedirs(backup_archive)

    if backup_archive == "/":
        LOG.error("The archive path should not be /")
        return False

    for backup_path in backup_path_list:
        if not os.path.isdir(backup_path):
            LOG.error("The directory %s does not exist", backup_path)
            return False

        if not os.listdir(backup_path):
            LOG.error("The directory %s is empty", backup_path)
            return False

        if backup_path == "/":
            LOG.error("The backup path should not be /")
            return False

        if backup_path in backup_archive:
            LOG.error("The archive folder %s should not inside the backup folder %s",
                      backup_archive, backup_path)
            return False

        if backup_archive in backup_path:
            LOG.error("The backup folder %s should not inside the archive folder %s",
                      backup_path, backup_archive)
            return False

    if not os.path.exists(SCRIPT_CONFIG):
        LOG.error("The %s does not exist", SCRIPT_CONFIG)
        return False
    LOG.info("Check config success")
    return True


def init_util_config(obsutil_config_file, retry_times):
    """初始化配置设置obsutil的参数
    :return:
    """
    if os.path.exists(obsutil_config_file):
        # pylint: disable=anomalous-backslash-in-string
        # change default retry time
        cmd = "sed -i \"/maxRetryCount=/ c\maxRetryCount=%d\" %s" % \
              (retry_times, obsutil_config_file)
        if execute_cmd(cmd) is False:
            LOG.error("Set retry_times to %d failed", retry_times)
            sys.exit(EXIT_ERROR)
        # change default log path
        cmd = "sed -i \"s@/root@%s@\" %s" % (LOG_DIR, obsutil_config_file)
        if execute_cmd(cmd) is False:
            LOG.error("Set obsutil log directory to %s failed", LOG_DIR)
            sys.exit(EXIT_ERROR)
    else:
        LOG.error("Check obsutil config failed, exit script")
        sys.exit(EXIT_ERROR)

    LOG.info("Check obsutil config success")


def get_logger():
    """获取日志句柄
    :return:
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(level=LOGGING_LEVEL)
    handler_file = logging.handlers.RotatingFileHandler(OBSUTIL_LOG_FILE, 'a', 10 * 1024 * 1024, 4)
    formatter = logging.Formatter('%(asctime)s %(process)d %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                                  '%Y-%m-%d %H:%M:%S %Z')
    handler_file.setFormatter(formatter)
    handler_file.setLevel(level=LOGGING_LEVEL)
    handler_sys = logging.StreamHandler()
    handler_sys.setLevel(level=LOGGING_LEVEL)
    logger.addHandler(handler_file)
    logger.addHandler(handler_sys)
    return logger


def generate_path_bytime():
    """根据本地时间生成obs上的路径
    :return: 返回生成的obs上的路径
    """
    time_now = datetime.datetime.now()
    obs_path = "%d%02d/%02d" % (time_now.year, time_now.month, time_now.day)
    LOG.info("Generate obs path success:%s", obs_path)
    return obs_path


def execute_cmd(cmd, is_trip=True, check_error=True):
    """执行cmd命令
    :param cmd: 要执行的命令
    :param is_trip: 返回结果是否需要过滤左右两侧的空格
    :param check_error: 执行命令是否检查stderr返回
    :return: 返回为False表示命令执行失败，命令执行成功返回执行命令的回显
    """
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()
    if is_trip:
        data = stdout.lstrip().rstrip()
    else:
        data = stdout
    result = {"data": data}
    if proc.stdin:
        proc.stdin.close()
    if proc.stdout:
        proc.stdout.close()
    if proc.stderr:
        proc.stderr.close()
    if check_error and stderr:
        LOG.error("execute command %s failed! Failed reason: %s", cmd, stderr)
        return False
    LOG.debug("execute command %s success!", cmd)
    return result


def process_exist():
    """检查是否有上传任务，有的话直接退出
    ：param： 无
    :return: 无
    """
    cmd = "ps -ef | grep -w python | grep -w obsutil_adapter.py | " \
          "grep -v grep | grep -v '/bin/sh -c' | wc -l"
    result = int(execute_cmd(cmd)["data"])
    if result > 1:
        LOG.error("The upload process is executing, the script will exit")
        sys.exit(EXIT_ERROR)


def delete_archive_result_file(conf):
    """删除归档目录和result目录中的超时文件
    :return:
    """
    backup_archive = conf.get("directory", "backup_archive")
    reserve_time = conf.getint("base", "reserve_time")
    delete_files(backup_archive, reserve_time)


def delete_files(directory, reserve_time, print_log=True):
    """删除指定目录中超过保留时长的文件
    ：param directory： 待删除的目录
    ：param reserve_time：保留时间
    ：param print_log：是否打印日志
    :return: 无
    """
    if not os.listdir(directory):
        if print_log:
            LOG.info("The directory %s is empty, no file to be deleted.", directory)
        return

    time_now = int(time.time())
    for (dirpath, _, filenames) in os.walk(directory):
        # pylint: disable=redefined-builtin
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            modify_time = int(os.stat(file_path).st_mtime)
            if time_now - modify_time > 60 * reserve_time:
                if print_log:
                    LOG.info("The file %s is modified %d minute ago, it will be deleted.",
                             file_path, reserve_time)
                os.remove(file_path)


# pylint: disable=too-many-locals
def upload(conf):
    """上传文件夹到obs
    :param:
    :return:
    """
    LOG.info("Start to upload files")
    obs_path = conf.get("obs", "obs_path")
    time_path = generate_path_bytime()
    obs_full_path = os.path.join(obs_path, time_path)
    obs_address = "obs://" + obs_full_path

    backup_path_list = conf.get("directory", "backup_path").replace(' ', '')
    backup_archive = conf.get("directory", "backup_archive")
    checkpoint_dir = os.path.join(LOG_DIR, ".obsutil_checkpoint")
    output_dir = os.path.join(LOG_DIR, ".obsutil_output")
    upload_time = get_upload_time(conf)
    exclude_file = "*" + HIDDEN_FILENAME

    cmd = "./obsutil cp %s %s -arcDir=%s -threshold=%s -ps=%s -cpd=%s -o=%s -msm=1 -f -r \
          -vlength -timeRange=*-%s -exclude=%s >> %s 2>&1; \
          echo $?" \
          % (backup_path_list, obs_address, backup_archive, THRESHOLD, THRESHOLD,
             checkpoint_dir, output_dir, upload_time, exclude_file, OBSUTIL_LOG_FILE)
    LOG.info("Upload command: %s", cmd)
    result = execute_cmd(cmd, check_error=False)
    LOG.info("Return code is: %s", result["data"])
    return_code = str(result["data"].decode("utf-8"))
    if return_code == "0":
        LOG.info("Upload %s to obs success", backup_path_list)
    else:
        LOG.error("Upload %s failed", backup_path_list)
        sys.exit(EXIT_ERROR)

    LOG.info("Upload files finished")


# return utc time now in obsutil time range format
def get_upload_time(conf):
    """
    :param conf:
    :return:
    """
    utc_now = datetime.datetime.utcnow()
    LOG.info("Current UTC time is %s", utc_now)
    modified_interval = 0 - conf.getint("base", "modified_interval")
    utc_upload = utc_now + datetime.timedelta(minutes=modified_interval)
    LOG.info("Upload files before %s", utc_upload)
    return utc_upload.strftime('%Y%m%d%H%M%S')


# create hidden file in update folder so that...
# 1. folder structure will not be removed after archive process
# 2. obsutil will not return error if one of the folders is enpty
def create_hidden_file(conf):
    """
    :param conf:
    :return:
    """
    backup_path_list = conf.get("directory", "backup_path").replace(' ', '').split(',')
    for root_folder in backup_path_list:
        for parent, dirname, _ in os.walk(root_folder):
            # create hidden file if leaf directory has no child directory
            if not dirname:
                hidden_file_path = os.path.join(parent, HIDDEN_FILENAME)
                if not os.path.exists(hidden_file_path):
                    try:
                        open(hidden_file_path, "w+").close()
                    except IOError as e:
                        LOG.error("Create file error in %s: %s", hidden_file_path, e)
                        sys.exit(EXIT_ERROR)
    LOG.info("Check folder finished")


if __name__ == '__main__':
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)
    LOG = get_logger()
    CONFIG_FILE = os.path.join("/root", OBSUTIL_CONFIG)

    process_exist()

    SCRIPT_CONF = get_config()
    if not check_config(SCRIPT_CONF):
        LOG.error("Check config failed, exit script")
        sys.exit(EXIT_ERROR)
    UPLOAD_RETRY_TIMES = SCRIPT_CONF.getint("base", "retry_times")

    init_util_config(CONFIG_FILE, UPLOAD_RETRY_TIMES)
    create_hidden_file(SCRIPT_CONF)
    delete_archive_result_file(SCRIPT_CONF)

    upload(SCRIPT_CONF)

    LOG.info("Upload script finished")
