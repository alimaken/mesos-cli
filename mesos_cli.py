import argparse
import colorama
import datetime
import os
import requests
#import sys
import time
from datasize import DataSize
from tabulate import tabulate
from colorama import Fore, Back, init, Style

class MesosMetrics(object):

    resources_master_cpus_percent = None
    resources_master_cpus_total = None
    resources_master_cpus_used = None
    resources_master_mem_percent = None
    resources_master_mem_total = None
    resources_master_mem_used = None
    messages_master_messages_deactivate_framework = None
    messages_master_messages_decline_offers = None
    messages_master_messages_executor_to_framework = None
    messages_master_messages_exited_executor = None
    messages_master_messages_framework_to_executor = None
    messages_master_messages_kill_task = None
    messages_master_messages_launch_tasks = None
    messages_master_messages_register_framework = None
    messages_master_messages_reregister_framework = None
    messages_master_messages_reregister_slave = None
    messages_master_messages_unregister_framework = None
    messages_master_messages_unregister_slave = None
    tasks_master_task_failed_source_slave_reason_container_launch_failed = 0
    tasks_master_task_failed_source_slave_reason_container_limitation_memory = 0
    tasks_master_task_killed_source_master_reason_framework_removed = 0
    tasks_master_task_lost_source_master_reason_slave_removed = 0
    tasks_master_task_running_source_executor_reason_task_health_check_status_updated = 0
    tasks_master_task_unreachable_source_master_reason_slave_removed = 0
    tasks_master_tasks_dropped = None
    tasks_master_tasks_error = None
    tasks_master_tasks_failed = None
    tasks_master_tasks_finished = None
    tasks_master_tasks_gone = None
    tasks_master_tasks_gone_by_operator = None
    tasks_master_tasks_killed = None
    tasks_master_tasks_killing = None
    tasks_master_tasks_lost = None
    tasks_master_tasks_running = None
    tasks_master_tasks_staging = None
    tasks_master_tasks_starting = None
    tasks_master_tasks_unreachable = None
    master_uptime_secs = None
    master_frameworks_active = None
    master_frameworks_connected = None
    master_frameworks_disconnected = None
    master_frameworks_inactive = None
    master_outstanding_offers = None

    def print_metrics(self):
        print(tabulate([
            [ Fore.GREEN + "MASTER" + Style.RESET_ALL,
            "Uptime: " + str(datetime.timedelta(seconds=self.master_uptime_secs)),
            "Outstanding Offers: " + str(int(self.master_outstanding_offers)),],
            [ Fore.GREEN + "FRAMEWORKS" + Style.RESET_ALL],[
            "Active: " + str(int(self.master_frameworks_active)),
            "Connected: " + str(int(self.master_frameworks_connected)),
            "Disconnected: " + str(int(self.master_frameworks_disconnected)),
            "Inactive: " + str(int(self.master_frameworks_inactive)),
            ],
            [ Fore.GREEN + "RESOURCES" + Style.RESET_ALL],[
                "cpus %: " + str(self.resources_master_cpus_percent),
                "cpus total: " + str(int(self.resources_master_cpus_total)),
                "cpus used: " + str(int(self.resources_master_cpus_used))],[
                "mem %: " + str(self.resources_master_mem_percent),
                "mem total: " + str(int(self.resources_master_mem_total)),
                "mem used: " + str(int(self.resources_master_mem_used)),
            ],
            [ Fore.GREEN + "MESSAGES" + Style.RESET_ALL],[
                "deactivate_framework: " + str(int(self.messages_master_messages_deactivate_framework)),
                "decline_offers: " + str(int(self.messages_master_messages_decline_offers)),
                "executor_to_framework: " + str(int(self.messages_master_messages_executor_to_framework)),
                "exited_executor: " + str(int(self.messages_master_messages_exited_executor))],[
                "framework_to_executor: " + str(int(self.messages_master_messages_framework_to_executor)),
                "kill_task: " + str(int(self.messages_master_messages_kill_task)),
                "launch_tasks: " + str(int(self.messages_master_messages_launch_tasks)),
                "register_framework: " + str(int(self.messages_master_messages_register_framework))],[
                "reregister_framework: " + str(int(self.messages_master_messages_reregister_framework)),
                "reregister_slave: " + str(int(self.messages_master_messages_reregister_slave)),
                "unregister_framework: " + str(int(self.messages_master_messages_unregister_framework)),
                "unregister_slave: " + str(int(self.messages_master_messages_unregister_slave)),
            ],
            [ Fore.GREEN + "TASKS" + Style.RESET_ALL],[
                "dropped: " + str(int(self.tasks_master_tasks_dropped))],[
                "error: " + str(int(self.tasks_master_tasks_error))],[
                "failed: " + str(int(self.tasks_master_tasks_failed)), "Reasons:",
                "slave_launch_failed: " + str(int(self.tasks_master_task_failed_source_slave_reason_container_launch_failed)),
                "slave_limitation_memory: " + str(int(self.tasks_master_task_failed_source_slave_reason_container_limitation_memory))],[
                "finished: " + str(int(self.tasks_master_tasks_finished))],[
                "killed: " + str(int(self.tasks_master_tasks_killed)),"Reasons:",
                "master_removed_framework: " + str(int(self.tasks_master_task_killed_source_master_reason_framework_removed))],[
                "killing: " + str(int(self.tasks_master_tasks_killing))],[
                "lost: " + str(int(self.tasks_master_tasks_lost)),"Reasons:",
                "slave_removed: " + str(int(self.tasks_master_task_lost_source_master_reason_slave_removed))],[
                "running: " + str(int(self.tasks_master_tasks_running))],[
                "staging: " + str(int(self.tasks_master_tasks_staging))],[
                "starting: " + str(int(self.tasks_master_tasks_starting))],[
                "unreachable: " + str(int(self.tasks_master_tasks_unreachable)),"Reasons:",
                "slave_removed: " + str(int(self.tasks_master_task_unreachable_source_master_reason_slave_removed)),
            ]
        ], tablefmt="rst"))

    def parse(self, metrics):

        # RESOURCES

        self.resources_master_cpus_percent = metrics["master/cpus_percent"]
        self.resources_master_cpus_total = metrics["master/cpus_total"]
        self.resources_master_cpus_used = metrics["master/cpus_used"]
        self.resources_master_mem_percent = metrics["master/mem_percent"]
        self.resources_master_mem_total = metrics["master/mem_total"]
        self.resources_master_mem_used = metrics["master/mem_used"]

        # MESSAGES

        self.messages_master_messages_deactivate_framework = metrics["master/messages_deactivate_framework"]
        self.messages_master_messages_decline_offers = metrics["master/messages_decline_offers"]
        self.messages_master_messages_executor_to_framework = metrics["master/messages_executor_to_framework"]
        self.messages_master_messages_exited_executor = metrics["master/messages_exited_executor"]
        self.messages_master_messages_framework_to_executor = metrics["master/messages_framework_to_executor"]
        self.messages_master_messages_kill_task = metrics["master/messages_kill_task"]
        self.messages_master_messages_launch_tasks = metrics["master/messages_launch_tasks"]
        self.messages_master_messages_register_framework = metrics["master/messages_register_framework"]
        self.messages_master_messages_reregister_framework = metrics["master/messages_reregister_framework"]
        self.messages_master_messages_reregister_slave = metrics["master/messages_reregister_slave"]
        self.messages_master_messages_unregister_framework = metrics["master/messages_unregister_framework"]
        self.messages_master_messages_unregister_slave = metrics["master/messages_unregister_slave"]

        # TASKS

        if "master/task_failed/source_slave/reason_container_launch_failed" in metrics:
            self.tasks_master_task_failed_source_slave_reason_container_launch_failed = metrics["master/task_failed/source_slave/reason_container_launch_failed"]
        if "master/task_failed/source_slave/reason_container_limitation_memory" in metrics:
            self.tasks_master_task_failed_source_slave_reason_container_limitation_memory = metrics["master/task_failed/source_slave/reason_container_limitation_memory"]
        if "master/task_killed/source_master/reason_framework_removed" in metrics:
            self.tasks_master_task_killed_source_master_reason_framework_removed = metrics["master/task_killed/source_master/reason_framework_removed"]
        if "master/task_lost/source_master/reason_slave_removed" in metrics:
            self.tasks_master_task_lost_source_master_reason_slave_removed = metrics["master/task_lost/source_master/reason_slave_removed"]
        if "master/task_running/source_executor/reason_task_health_check_status_updated" in metrics:
            self.tasks_master_task_running_source_executor_reason_task_health_check_status_updated = metrics["master/task_running/source_executor/reason_task_health_check_status_updated"]
        if "master/task_unreachable/source_master/reason_slave_removed" in metrics:
            self.tasks_master_task_unreachable_source_master_reason_slave_removed = metrics["master/task_unreachable/source_master/reason_slave_removed"]
        self.tasks_master_tasks_dropped = metrics["master/tasks_dropped"]
        self.tasks_master_tasks_error = metrics["master/tasks_error"]
        self.tasks_master_tasks_failed = metrics["master/tasks_failed"]
        self.tasks_master_tasks_finished = metrics["master/tasks_finished"]
        self.tasks_master_tasks_gone = metrics["master/tasks_gone"]
        self.tasks_master_tasks_gone_by_operator = metrics["master/tasks_gone_by_operator"]
        self.tasks_master_tasks_killed = metrics["master/tasks_killed"]
        self.tasks_master_tasks_killing = metrics["master/tasks_killing"]
        self.tasks_master_tasks_lost = metrics["master/tasks_lost"]
        self.tasks_master_tasks_running = metrics["master/tasks_running"]
        self.tasks_master_tasks_staging = metrics["master/tasks_staging"]
        self.tasks_master_tasks_starting = metrics["master/tasks_starting"]
        self.tasks_master_tasks_unreachable = metrics["master/tasks_unreachable"]

        # MASTER

        self.master_uptime_secs = metrics["master/uptime_secs"]
        self.master_frameworks_active = metrics["master/frameworks_active"]
        self.master_frameworks_connected = metrics["master/frameworks_connected"]
        self.master_frameworks_disconnected = metrics["master/frameworks_disconnected"]
        self.master_frameworks_inactive = metrics["master/frameworks_inactive"]
        self.master_outstanding_offers = metrics["master/outstanding_offers"]

    def __init__(self):
        pass

class MesosParser(object):
    # os.system('clear')
    # url = "http://odhecx52:5040/master/frameworks"
    # http://odhecx52:5040/metrics/snapshot 
    frameworks_url = None
    metrics_url = None
    app_name = None
    framework_id = None
    host = "odhecx52"
    port = "5040"
    watcher = None

    def __init__(self):

        colorama.init(autoreset=True)

        parser = argparse.ArgumentParser(description = "Mesos Master Status Parser")

        parser.add_argument("-H", "--Help", help = "Example: Help argument", required = False, default = "")
        parser.add_argument("-a", "--app", help = "Application Name", required = False, default = "")
        parser.add_argument("-m", "--master", help = "Mesos Master Host Name", required = False, default = self.host)
        parser.add_argument("-p", "--port", help = "Port Number", required = False, default = self.port)
        parser.add_argument("-w", "--watch", help = "Enable Watcher", required = False, default = "", action='store_true')

        argument = parser.parse_args()

        if argument.Help:
            print("You have used '-H' or '--Help' with argument: {0}".format(argument.Help))
        
        if argument.app:
            self.app_name = argument.app

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

        self.framework_id = self.get_framework_id(self.frameworks_url, self.app_name)


        if self.watcher:
            while(True):
                os.system('clear')
                self.print_results(self.frameworks_url, self.framework_id)
                for i in range(0,5): 
                    print(".", end="", flush=True)
                    time.sleep(1)
        else:
            self.print_results(self.frameworks_url, self.framework_id)

    def print_legend(self):
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
        metrics_parsed = mesos_metrics.parse(metrics)
        mesos_metrics.print_metrics()



        data = self.get_json_by_rest(url, framework_id)
        # print(data)
        frameworks = data["frameworks"]

        if self.app_name and len(frameworks) != 1:
            self.framework_id = self.get_framework_id(self.frameworks_url, self.app_name)
        table = []
        if self.app_name:
            print("Watching framework: ", self.app_name)
        print("Fetching frameworks from: ",self.frameworks_url, "Total: ", len(frameworks))
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
            uptime_discriptive = str(datetime.timedelta(seconds=diff))
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
                Fore.GREEN + uptime_discriptive,
                Fore.WHITE + url,
                Fore.GREEN + self.get_tasks_str(tasks)
            ])


        print(tabulate(sorted(table,
                              key=lambda x: x[0]),
                       headers=['s/n','framework', 'memory', '#cpu', '#tasks', 'up_since', 'uptime', 'url', 'tasks'],
                       tablefmt="rst",
                       showindex="always"))

    def get_url_for_frameworks(self, host, port):
        return "http://{}:{}/master/frameworks".format(host, port)

    def get_url_for_metrics(self, host, port):
        return "http://{}:{}/metrics/snapshot".format(host, port)

    def to_short_status(self, status):
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
                task_mem  = str(int(task["resources"]["mem"]))
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
            return tabulate(table,tablefmt="plain")

    def get_json_by_rest(self, url_path, framework_id = None):
        # url = "http://odhecx52:5040/master/frameworks?framework_id=d819d33b-0495-4269-a6ce-23fbe37ca6aa-90314"        
        if framework_id:
            url_path = url_path + "?framework_id={}".format(framework_id)
        resp = requests.get(url=url_path)
        data = resp.json()
        return data

    def get_framework_id(self, url, app_name):
        if app_name:
            res = self.get_json_by_rest(url)
            if res:
                for framework in res["frameworks"]:
                    if framework["name"] == app_name:
                        print("Framework ID: ", framework["id"])
                        return framework["id"]
            return "SomeFramwork"
        else:
            return None







if __name__ == '__main__':
    app = MesosParser()

####
## Colorama Options
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
