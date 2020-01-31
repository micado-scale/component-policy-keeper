import logging
import json
import time
from threading import Thread

import docker

import pk_config

dryrun_id = "terraform"

# These fields should exist in PK's config.yaml
CONFIG_CONTAINER_NAME = "terraform_container_name"
CONFIG_PATH = "terraform_path"
TF_VARS_PATH = "/submitter/terraform.tfvars.json"

# Use this to log to the Terraform container
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
    counts = read_vars_file()
    for info in scaling_info_list:
        replicas = info.get("replicas")
        node = info.get("node_name")
        var_name = node + "-count"
        counts[var_name] = replicas
        log.info("(S) {}  => m_node_count: {}".format(node, replicas))
    write_vars_file(counts)

    # Build the Terraform CLI command to execute with Docker
    shell_command = "terraform apply --auto-approve"
    shell_command += LOG_SUFFIX
    exec_command = ["sh", "-c", shell_command]
    log.debug("-->docker exec {}".format(exec_command))

    # Thread this action so PK can continue, Terraform has its own lock
    Thread(target=perform_scaling, args=(config, exec_command)).start()


def query_number_of_worker_nodes(config, worker_name):
    instances = 1
    if pk_config.dryrun_get(dryrun_id):
        log.info("(C)   DRYRUN enabled. Skipping...")
        return instances
    try:
        resources = get_resources_from_state(config, worker_name)
        instances = len(resources[0]["instances"])
    except Exception:
        log.error("Failed to get no. of instances for {}".format(worker_name))
    log.debug("-->instances: {0}".format(instances))
    return instances


def drop_worker_node(config, scaling_info_list):
    # TODO drop specific worker
    pass


def get_terraform(container_name):
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


def get_resources_from_state(config, worker_name):
    container_name = config.get(CONFIG_CONTAINER_NAME, "terraform")
    terra_path = config.get(CONFIG_PATH)

    terraform = get_terraform(container_name)
    command = ["terraform", "state", "pull"]

    # Parse Terraform's state file
    out = terraform.exec_run(command, workdir=terra_path)
    if out.exit_code != 0 or not out.output:
        log.error("Could not get Terraform state")
    json_state = json.loads(out.output.decode())

    # Return the resource matching the given worker
    resources = json_state.get("resources")
    resources = [x for x in resources if x.get("name") == worker_name]
    if not resources:
        log.error("No resources matching {}".format(worker_name))
    return resources


def perform_scaling(config, command):
    container_name = config.get(CONFIG_CONTAINER_NAME, "terraform")
    terra_path = config.get(CONFIG_PATH)

    # Run the command in the container
    terraform = get_terraform(container_name)
    try:
        exec_run = terraform.exec_run(command, workdir=terra_path)
        log.debug("-->exit code {}".format(exec_run.exit_code))
    except Exception:
        log.error("Failed to scale {}")


def read_vars_file():
    with open(TF_VARS_PATH, "r") as file:
        data = json.load(file)
    return data


def write_vars_file(data):
    with open(TF_VARS_PATH, "w") as file:
        json.dump(data, file, indent=4)
