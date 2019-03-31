import argparse
import datetime
import re
import os
import time

import colorama
import requests
from colorama import Fore, Back, Style
from datasize import DataSize
from tabulate import tabulate

from mesos_metrics import MesosMetrics


class MesosParser(object):
    # os.system('clear')
    # url = "http://odhecx52:5040/master/frameworks"
    # http://odhecx52:5040/metrics/snapshot 
    frameworks_url = None
    metrics_url = None
    app_name = None
    app_name_regex = None
    framework_id = None
    host = "odhecx52"
    port = "5040"
    watcher = None
    cmd_line = []

    def __init__(self):

        colorama.init(autoreset=True)

        parser = argparse.ArgumentParser(description="Mesos Master Status Parser")

        parser.add_argument("-a", "--app", help="Application Name", required=False, default="")
        parser.add_argument("-r", "--regex", help="Application Name Regex", required=False, default="")
        parser.add_argument("-m", "--master", help="Mesos Master Host Name", required=False, default=self.host)
        parser.add_argument("-p", "--port", help="Port Number", required=False, default=self.port)
        parser.add_argument("-w", "--watch", help="Enable Watcher", required=False, default="", action='store_true')

        argument = parser.parse_args()
        self.print_cmd_params(argument)


        if argument.app:
            self.app_name = argument.app

        if argument.regex:
            self.app_name_regex = argument.regex

        if argument.master:
            self.host = argument.master
            
        if argument.port:
            self.port = argument.port

        if argument.watch:
            self.watcher = True
        
        self.frameworks_url = self.get_url_for_frameworks(self.host, self.port)
        self.metrics_url = self.get_url_for_metrics(self.host, self.port)
        
        # url = "http://odhecx52:5040/master/frameworks?framework_id=d819d33b-0495-4269-a6ce-23fbe37ca6aa-90314"
        # self.url = "http://odhecx52:5040/master/frameworks"

        self.framework_id = self.get_framework_id(self.frameworks_url, self.app_name, self.app_name_regex)

        if self.watcher:
            while True:
                os.system('clear')
                self.print_results(self.frameworks_url, self.framework_id)
                for i in range(0, 5):
                    print(".", end="", flush=True)
                    time.sleep(1)
        else:
            self.print_results(self.frameworks_url, self.framework_id)

    @staticmethod
    def print_legend():
        print(tabulate([[
            "[" + Fore.BLACK + Back.RED + "FLD" + Style.RESET_ALL + "] = TASK_FAILED",
            "[" + Fore.BLACK + Back.CYAN + "FIN" + Style.RESET_ALL + "] = TASK_FINISHED",
            "[" + Fore.BLACK + Back.RED + "KLD" + Style.RESET_ALL + "] = TASK_KILLED",
            "[" + Fore.BLACK + Back.RED + "KIL" + Style.RESET_ALL + "] = TASK_KILLING",
            "[" + Fore.BLACK + Back.RED + "LST" + Style.RESET_ALL + "] = TASK_LOST",
            "[" + Fore.GREEN + "RUN" + Style.RESET_ALL + "] = TASK_RUNNING",
            "[" + Fore.BLACK + Back.YELLOW + Style.DIM + "STR" + Style.RESET_ALL + "] = TASK_STARTING",
            "[" + Fore.BLACK + Back.RED + "URC" + Style.RESET_ALL + "] = TASK_UNREACHABLE",
            "[" + Fore.BLACK + Back.YELLOW + Style.DIM + "STG" + Style.RESET_ALL + "] = TASK_STAGING",
        ]], tablefmt="pipe"))

    def print_results(self, url, framework_id):

        metrics = self.get_json_by_rest(self.metrics_url)
        mesos_metrics = MesosMetrics()
        mesos_metrics.parse(metrics)
        mesos_metrics.print_metrics()

        data = self.get_json_by_rest(url, framework_id)
        frameworks = data["frameworks"]

        if self.app_name and len(frameworks) != 1:
            self.framework_id = self.get_framework_id(self.frameworks_url, self.app_name, self.app_name_regex)
        table = []
        if self.app_name:
            print("Watching framework: ", self.app_name)
        print("Fetching frameworks from: ", self.frameworks_url, "Total: ", len(frameworks))
        print("Fetching metrics from: ", self.metrics_url)
        self.print_legend()

        for framework in frameworks:
            resources = framework["resources"]
            name = framework["name"]
            if name == "marathon" or name == "chronos":
                continue
            memory = str(int(resources["mem"])) + "MB"
            cpus = int(resources["cpus"])
            url = framework["webui_url"]
            tasks = framework["tasks"]
            tasks_len = str(len(tasks))
            ts_epoch = int(framework["registered_time"])
            now = time.time()
            diff = (now - ts_epoch)
            uptime = datetime.datetime.fromtimestamp(ts_epoch).strftime('%Y-%m-%d %H:%M:%S')
            uptime_descriptive = str(datetime.timedelta(seconds=diff))
            if cpus < 1:
                color = Fore.RED
            else:
                color = Fore.GREEN

            table.append([
                color + name,
                Fore.MAGENTA + "{:.2GiB}".format(DataSize(memory)),
                Fore.CYAN + str(cpus),
                Fore.YELLOW + tasks_len,
                Fore.GREEN + uptime,
                Fore.GREEN + uptime_descriptive,
                Fore.WHITE + url,
                Fore.GREEN + self.get_tasks_str(tasks)
            ])

        print(tabulate(sorted(table,
                              key=lambda x: x[0]),
                       headers=['s/n', 'framework', 'memory', '#cpu', '#tasks', 'up_since', 'uptime', 'url', 'tasks'],
                       tablefmt="rst",
                       showindex="always"))

    @staticmethod
    def get_url_for_frameworks(host, port):
        return "http://{}:{}/master/frameworks".format(host, port)

    @staticmethod
    def get_url_for_metrics(host, port):
        return "http://{}:{}/metrics/snapshot".format(host, port)

    @staticmethod
    def to_short_status(status):
            switcher = {
                "TASK_STAGING": Fore.BLACK + Back.YELLOW + Style.DIM + "STG" + Style.RESET_ALL,
                "TASK_STARTING": Fore.BLACK + Back.YELLOW + Style.DIM + "STR" + Style.RESET_ALL,
                "TASK_RUNNING": Fore.GREEN + "RUN" + Style.RESET_ALL,
                "TASK_UNREACHABLE": Fore.BLACK + Back.RED + "URC" + Style.RESET_ALL,
                "TASK_KILLING": Fore.BLACK + Back.RED + "KIL" + Style.RESET_ALL,
                "TASK_FINISHED": Fore.BLACK + Back.CYAN + "FIN" + Style.RESET_ALL,
                "TASK_KILLED": Fore.BLACK + Back.RED + "KLD" + Style.RESET_ALL,
                "TASK_FAILED": Fore.BLACK + Back.RED + "FLD" + Style.RESET_ALL,
                "TASK_LOST": Fore.BLACK + Back.RED + "LST" + Style.RESET_ALL
            }
            return switcher.get(status, "-")

    def get_tasks_str(self, tasks):
            tasks_lst = []
            for task in tasks:
                task_id = task["id"]
                task_state = self.to_short_status(task["state"])
                task_mem = str(int(task["resources"]["mem"]))
                task_cpus = task["resources"]["cpus"]
                tasks_lst.append("[{}]".format(
                        # task_id,
                        task_state,
                        # task_mem,
                        # int(task_cpus)
                    ))
            tasks_lst_len = len(tasks_lst)
            if tasks_lst_len < 5:
                table = [tasks_lst]
            else:        
                table = [tasks_lst[x:x+5] for x in range(0, len(tasks_lst), 5)]
            return tabulate(table, tablefmt="plain")

    @staticmethod
    def get_json_by_rest(url_path, framework_ids=[]):
        # url = "http://odhecx52:5040/master/frameworks?framework_id=d819d33b-0495-4269-a6ce-23fbe37ca6aa-90314"
        if framework_ids and len(framework_ids) > 0:
            print("framework_ids are: ", framework_ids)
            data = {"frameworks": []}
            for framework_id in framework_ids:
                url_path_framework = url_path + "?framework_id={}".format(framework_id)
                resp = requests.get(url=url_path_framework)
                resp_json = resp.json()
                data["frameworks"].append(resp_json["frameworks"][0])
            return data
        resp = requests.get(url=url_path)
        data = resp.json()
        return data

    def get_framework_id(self, url, app_name, app_name_regex=None):
        if app_name_regex:
            frameworks_lst = []
            res = self.get_json_by_rest(url)
            if res:
                for framework in res["frameworks"]:
                    if re.search(app_name_regex, framework["name"]):
                        frameworks_lst.append(framework["id"])

            return frameworks_lst
        else:
            if app_name:
                res = self.get_json_by_rest(url)
                if res:
                    for framework in res["frameworks"]:
                        if framework["name"] == app_name:
                            return [framework["id"]]
                return ["SomeFramework"]
            else:
                return None

    @staticmethod
    def print_cmd_params(argument):
        for arg in vars(argument):
            print(arg, getattr(argument, arg))


if __name__ == '__main__':
    app = MesosParser()

####
# Colorama Options
####
# Fore: BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, RESET.
# Back: BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, RESET.
# Style: DIM, NORMAL, BRIGHT, RESET_ALL

####
# Table Styles
####
# plain
# simple
# grid
# fancy_grid
# pipe
# orgtbl
# jira
# presto
# psql
# rst
# mediawiki
# moinmoin
# youtrack
# html
# latex
# latex_raw
# latex_booktabs
# textile
