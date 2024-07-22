from kubernetes import config as kubeconfig
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

kubeconfig.load_kube_config()
api = core_v1_api.CoreV1Api()

class KubeTerraform():

    def __init__(deployment_name, namespace):
        """Initialise kubernetes sdk from kubeconfig"""
        self.deployment = deployment_name
        self.namespace = namespace

    def _get_pod():
        """Get the pod from the deployment"""
        if not api:
            raise NameError("Kube API not initialised!")
        try:
            return [
                pod.metadata.name
                for pod in api.list_namespaced_pod(self.namespace).items
                if pod.metadata.name.startswith(self.deployment)
            ][0]
        except IndexError as e:
            logger.error(f"Could not find {self.deployment} pod in {self.namespace}!")
            raise e from None

    def exec_run(command, success=None):
        """Exec a shell command in the pod, optionally check success"""
        pod_name = _get_pod()
        exec_command = ["/bin/sh", "-c"]
        exec_command.append(command)
        try:
            resp = stream(
                api.connect_get_namespaced_pod_exec,
                pod_name,
                namespace,
                command=exec_command,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
            if success and success not in resp:
                logger.error(f"{pod_name} exec error: {resp}")
                raise AdaptorCritical(f"Error: {resp}")
        except ApiException as e:
            logger.error(f"K8s API error: {e}")
            raise AdaptorCritical(f"Error: {e}")
