import logging
import json
import time
import threading
from itertools import zip_longest

import docker

import pk_config

dryrun_id = "terraform"

CONFIG_CONTAINER_NAME = "terraform_container_name"
CONFIG_PATH = "terraform_path"
TF_VARS_PATH = "/submitter/terraform.tfvars.json"

# Append this to commands to log to the Terraform container
LOG_SUFFIX = (
    " | while IFS= read -r line;"
    ' do printf "%s %s\n" "$(date "+[%Y-%m-%d %H:%M:%S]")" "$line";'
    " done | tee /proc/1/fd/1"
)

log = logging.getLogger("pk_terraform")
client = docker.from_env()


def scale_worker_node(config, scaling_info_list):
    """
    Calculate the scaling variance and modify the vars file accordingly
    """
    if pk_config.dryrun_get(dryrun_id):
        log.info("(S)   DRYRUN enabled. Skipping...")
        return
    node_variables = _read_vars_file()
    for info in scaling_info_list:
        replicas = info.get("replicas")
        node_name = info.get("node_name")

        nodes = node_variables[node_name]
        variance = replicas - len(nodes)
        if variance > 0:
            _add_nodes(nodes, variance)
        elif variance < 0:
            _del_nodes(nodes, abs(variance))
        log.info("(S) {}  => m_node_count: {}".format(node_name, replicas))
    _write_vars_file(node_variables)

    # Thread this action so PK can continue
    if _thread_not_active():
        tf_thread = threading.Thread(
            target=perform_scaling, args=(config,), name="TerraformThread",
        )
        tf_thread.start()


def query_number_of_worker_nodes(config, worker_name):
    """
    Return the number of instances of a worker node, pulled from tfstate
    """
    instances = 1
    if pk_config.dryrun_get(dryrun_id):
        log.info("(C)   DRYRUN enabled. Skipping...")
        return instances
    try:
        resources = _get_resources_from_state(config, worker_name)
        instances = len(resources[0]["instances"])
    except Exception:
        log.error("Failed to get no. of instances for {}".format(worker_name))
    log.debug("-->instances: {0}".format(instances))
    return instances


def drop_worker_node(config, scaling_info_list):
    """
    Scan for a specific worker node IP and drop it
    """
    indices = []
    for info in scaling_info_list:
        ips_to_drop = info.get("replicas")
        node_name = info.get("node_name")
        if not ips_to_drop:
            continue

        ip_list = _get_ips_from_output(config, node_name)
        if not ip_list:
            continue

        for target_ip in ips_to_drop:
            indices.append(_get_target_by_ip(ip_list, target_ip))

        _drop_nodes_by_indices(node_name, indices)

    if _thread_not_active():
        tf_thread = threading.Thread(
            target=perform_scaling, args=(config,), name="TerraformThread",
        )
        tf_thread.start()


def get_terraform(config):
    """
    Return the Terraform container
    """
    container_name = config.get(CONFIG_CONTAINER_NAME, "terraform")
    for i in range(1, 6):
        try:
            terraform = client.containers.list(
                filters={
                    "label": "io.kubernetes.container.name={}".format(container_name)
                }
            )[0]
            return terraform
        except Exception as e:
            log.debug("Failed attempt {}/5 attaching to Terraform: {}".format(i, e))
            time.sleep(5)
    log.error("Failed to get Terraform container")


def perform_scaling(config):
    """
    Execute the terraform apply command in the TF container
    """
    terraform = get_terraform(config)
    terra_path = config.get(CONFIG_PATH)

    shell_command = "terraform apply -auto-approve -no-color" + LOG_SUFFIX
    command = ["sh", "-cl", shell_command]

    exit_code, out = terraform.exec_run(command, workdir=terra_path)
    if exit_code > 0:
        log.error("Terraform exec failed {}".format(out))


def _get_json_from_command(config, command):
    """
    Return the JSON from a specific TF command
    """
    terraform = get_terraform(config)
    terra_path = config.get(CONFIG_PATH)

    out = terraform.exec_run(command, workdir=terra_path)
    if out.exit_code != 0 or not out.output:
        log.error("Could not get json data")
    return json.loads(out.output.decode())


def _get_ips_from_output(config, node):
    """
    Parse the IP info from the terraform output command's JSON
    """
    command = ["terraform", "output", "-json"]
    ip_info = _get_json_from_command(config, command)
    if not ip_info.get(node):
        return
    return ip_info[node]["value"]


def _get_resources_from_state(config, worker_name):
    """
    Return the resource matching the worker from the tfstate file
    """
    command = ["terraform", "state", "pull"]
    json_state = _get_json_from_command(config, command)

    resources = json_state.get("resources")
    resources = [x for x in resources if x.get("name") == worker_name]
    if not resources:
        log.error("No resources matching {}".format(worker_name))
    return resources


def _get_target_by_ip(ip_list, ip_to_drop):
    """
    Scan the instance IPs and return the index of the match to drop
    """
    privates = ip_list.get("private_ips", [])
    publics = ip_list.get("public_ips", [])

    for index, ips in enumerate(zip_longest(privates, publics)):
        if ip_to_drop in ips:
            return index

def _read_vars_file():
    """
    Return the data from the tfvars file
    """
    with open(TF_VARS_PATH, "r") as file:
        data = json.load(file)
    return data


def _write_vars_file(data):
    """
    Write data to the tfvars file
    """
    with open(TF_VARS_PATH, "w") as file:
        json.dump(data, file, indent=4)


def _thread_not_active():
    """
    Return true if a terraform apply is not already running
    """
    return "TerraformThread" not in [x.name for x in threading.enumerate()]

def _add_nodes(nodes, variance):
    """
    Scale up by adding node(s) to the infrastructure 
    """
    if not nodes:
        nodes.append("1")
        variance -= 1
    for _ in range(variance):
        node_name = str(int(nodes[-1])+1)
        nodes.append(node_name)

def _del_nodes(nodes, variance):
    """
    Scale down by removing node(s) from the infrastructure
    """
    for _ in range(variance):
        nodes.pop()

def _drop_nodes_by_indices(node_name, indices):
    """
    Drop specific nodes by removing them from the tfvars file
    """
    node_variables = _read_vars_file()
    for index in indices:
        node_variables[node_name].sort()
        node_variables[node_name].pop(index)
    _write_vars_file(node_variables)
