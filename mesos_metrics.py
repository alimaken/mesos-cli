import datetime

from colorama import Fore, Style
from tabulate import tabulate


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
            [Fore.GREEN + "MASTER" + Style.RESET_ALL,
                "Uptime: " + str(datetime.timedelta(seconds=self.master_uptime_secs)),
                "Outstanding Offers: " + str(int(self.master_outstanding_offers)), ],
            [Fore.GREEN + "FRAMEWORKS" + Style.RESET_ALL], [
                "Active: " + str(int(self.master_frameworks_active)),
                "Connected: " + str(int(self.master_frameworks_connected)),
                "Disconnected: " + str(int(self.master_frameworks_disconnected)),
                "Inactive: " + str(int(self.master_frameworks_inactive)),
            ],
            [Fore.GREEN + "RESOURCES" + Style.RESET_ALL], [
                "cpus %: " + str(self.resources_master_cpus_percent),
                "cpus total: " + str(int(self.resources_master_cpus_total)),
                "cpus used: " + str(int(self.resources_master_cpus_used))], [
                "mem %: " + str(self.resources_master_mem_percent),
                "mem total: " + str(int(self.resources_master_mem_total)),
                "mem used: " + str(int(self.resources_master_mem_used)),
            ],
            [Fore.GREEN + "MESSAGES" + Style.RESET_ALL], [
                "deactivate_framework: " + str(int(self.messages_master_messages_deactivate_framework)),
                "decline_offers: " + str(int(self.messages_master_messages_decline_offers)),
                "executor_to_framework: " + str(int(self.messages_master_messages_executor_to_framework)),
                "exited_executor: " + str(int(self.messages_master_messages_exited_executor))], [
                "framework_to_executor: " + str(int(self.messages_master_messages_framework_to_executor)),
                "kill_task: " + str(int(self.messages_master_messages_kill_task)),
                "launch_tasks: " + str(int(self.messages_master_messages_launch_tasks)),
                "register_framework: " + str(int(self.messages_master_messages_register_framework))], [
                "reregister_framework: " + str(int(self.messages_master_messages_reregister_framework)),
                "reregister_slave: " + str(int(self.messages_master_messages_reregister_slave)),
                "unregister_framework: " + str(int(self.messages_master_messages_unregister_framework)),
                "unregister_slave: " + str(int(self.messages_master_messages_unregister_slave)),
            ],
            [Fore.GREEN + "TASKS" + Style.RESET_ALL], [
                "dropped: " + str(int(self.tasks_master_tasks_dropped))], [
                "error: " + str(int(self.tasks_master_tasks_error))], [
                "failed: " + str(int(self.tasks_master_tasks_failed)), "Reasons:",
                "slave_launch_failed: " +
                str(int(self.tasks_master_task_failed_source_slave_reason_container_launch_failed)),
                "slave_limitation_memory: " +
                str(int(self.tasks_master_task_failed_source_slave_reason_container_limitation_memory))], [
                "finished: " + str(int(self.tasks_master_tasks_finished))], [
                "killed: " + str(int(self.tasks_master_tasks_killed)), "Reasons:",
                "master_removed_framework: " +
                str(int(self.tasks_master_task_killed_source_master_reason_framework_removed))], [
                "killing: " + str(int(self.tasks_master_tasks_killing))], [
                "lost: " + str(int(self.tasks_master_tasks_lost)), "Reasons:",
                "slave_removed: " + str(int(self.tasks_master_task_lost_source_master_reason_slave_removed))], [
                "running: " + str(int(self.tasks_master_tasks_running))], [
                "staging: " + str(int(self.tasks_master_tasks_staging))], [
                "starting: " + str(int(self.tasks_master_tasks_starting))], [
                "unreachable: " + str(int(self.tasks_master_tasks_unreachable)), "Reasons:",
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
            self.tasks_master_task_failed_source_slave_reason_container_launch_failed = \
                metrics["master/task_failed/source_slave/reason_container_launch_failed"]
        if "master/task_failed/source_slave/reason_container_limitation_memory" in metrics:
            self.tasks_master_task_failed_source_slave_reason_container_limitation_memory = \
                metrics["master/task_failed/source_slave/reason_container_limitation_memory"]
        if "master/task_killed/source_master/reason_framework_removed" in metrics:
            self.tasks_master_task_killed_source_master_reason_framework_removed = \
                metrics["master/task_killed/source_master/reason_framework_removed"]
        if "master/task_lost/source_master/reason_slave_removed" in metrics:
            self.tasks_master_task_lost_source_master_reason_slave_removed = \
                metrics["master/task_lost/source_master/reason_slave_removed"]
        if "master/task_running/source_executor/reason_task_health_check_status_updated" in metrics:
            self.tasks_master_task_running_source_executor_reason_task_health_check_status_updated = \
                metrics["master/task_running/source_executor/reason_task_health_check_status_updated"]
        if "master/task_unreachable/source_master/reason_slave_removed" in metrics:
            self.tasks_master_task_unreachable_source_master_reason_slave_removed = \
                metrics["master/task_unreachable/source_master/reason_slave_removed"]
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
