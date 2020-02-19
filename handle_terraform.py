import logging
import json
import time
import threading
from itertools import izip_longest

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
    if pk_config.dryrun_get(dryrun_id):
        log.info("(S)   DRYRUN enabled. Skipping...")
        return
    count_variables = _read_vars_file()
    for info in scaling_info_list:
        replicas = info.get("replicas")
        node = info.get("node_name")
        var_name = node + "-count"
        count_variables[var_name] = replicas
        log.info("(S) {}  => m_node_count: {}".format(node, replicas))
    _write_vars_file(count_variables)

    # Build the Terraform CLI command
    shell_command = "terraform apply --auto-approve" + LOG_SUFFIX
    exec_command = ["sh", "-c", shell_command]

    # Thread this action so PK can continue
    if _thread_not_active():
        tf_thread = threading.Thread(
            target=perform_scaling, args=(config, exec_command), name="TerraformThread"
        )
        tf_thread.start()


def query_number_of_worker_nodes(config, worker_name):
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
    destroy_targets = ""
    for info in scaling_info_list:
        ips_to_drop = info.get("replicas")
        node_name = info.get("node_name")
        if not ips_to_drop:
            continue

        ip_list = _get_ips_from_output(config, node_name)

        for target_ip in ips_to_drop:
            target = _get_target_by_ip(ip_list, target_ip)
            destroy_targets += "-target {} ".format(target)
    destroy_targets.strip()

    # Build the Terraform CLI command to execute with Docker
    shell_command = "terraform destroy --auto-approve " + destroy_targets + LOG_SUFFIX
    exec_command = ["sh", "-c", shell_command]

    if _thread_not_active():
        tf_thread = threading.Thread(
            target=perform_scaling,
            args=(config, exec_command),
            kwargs={"lock_timeout": 300},
            name="TerraformThread",
        )
        tf_thread.start()

def get_terraform(config):
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

def perform_scaling(config, command, lock_timeout=0):
    """ Execute the command in the terraform container """
    terraform = get_terraform(config)
    terra_path = config.get(CONFIG_PATH)

    while True:
        exit_code, out = terraform.exec_run(command, workdir=terra_path)
        if exit_code > 0:
            log.error("Terraform exec failed {}".format(out))
        elif lock_timeout > 0 and "Error locking state" in str(out):
            time.sleep(5)
            lock_timeout -= 5
            log.debug("Waiting for lock, {}s until timeout".format(lock_timeout))
        else:
            break

def _get_json_from_command(config, command):

    terraform = get_terraform(config)
    terra_path = config.get(CONFIG_PATH)

    out = terraform.exec_run(command, workdir=terra_path)
    if out.exit_code != 0 or not out.output:
        log.error("Could not get json data")
    return json.loads(out.output.decode())

def _get_ips_from_output(config, node):
    command = ["terraform", "output", "-json"]
    ip_info = _get_json_from_command(config, command)
    return ip_info[node]["value"]

def _get_resources_from_state(config, worker_name):
    # Return the resource matching the given worker
    command = ["terraform", "state", "pull"]
    json_state = _get_json_from_command(config, command)

    resources = json_state.get("resources")
    resources = [x for x in resources if x.get("name") == worker_name]
    if not resources:
        log.error("No resources matching {}".format(worker_name))
    return resources

def _get_target_by_ip(ip_list, ip_to_drop):
    target = ip_list.get("target")
    privates = ip_list.get("private_ips", [])
    publics = ip_list.get("public_ips", [])

    for index, ips in izip_longest(privates, publics):
        if ip_to_drop in ips:
            return target + "[{}]".format(index)

def _read_vars_file():
    with open(TF_VARS_PATH, "r") as file:
        data = json.load(file)
    return data

def _write_vars_file(data):
    with open(TF_VARS_PATH, "w") as file:
        json.dump(data, file, indent=4)

def _thread_not_active():
    return "TerraformThread" not in [x.name for x in threading.enumerate()]
