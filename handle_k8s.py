import kubernetes.client
import kubernetes.config
import logging
import pk_config
import time

def query_list_of_nodes(endpoint,status='ready'):
  log=logging.getLogger('pk_docker')
  list_of_nodes=[]
  if pk_config.simulate():
    return None
  kubernetes.config.load_kube_config()
  client = kubernetes.client.CoreV1Api()
  try:
    if status=='ready':
      nodes = [x for x in client.list_node().items if not x.spec.taints]
    elif status=='down':
      nodes = [x for x in client.list_node().items if x.spec.taints and 'master' not in x.spec.taints[0].key]
    for n in nodes:
      a = {}
      a['ID']=n.metadata.name
      a['Addr']=n.status.addresses[0].address
      list_of_nodes.append(a.copy())
    return list_of_nodes
  except Exception as e:
    log.exception('(Q) Query of k8s nodes failed.')
    return None

def scale_docker_service(endpoint,service_name,replicas):
  service_name = service_name.split('_')[1]
  log=logging.getLogger('pk_docker')
  log.info('(S) => m_container_count: {0}'.format(replicas))
  if pk_config.simulate():
    return
  kubernetes.config.load_kube_config()
  client = kubernetes.client.ExtensionsV1beta1Api()
  try:
    dep = client.read_namespaced_deployment(service_name, "default")
    dep.spec.replicas = replicas
    client.patch_namespaced_deployment_scale(service_name, "default", dep)
  except Exception as e:
    log.warning('(S) Scaling of k8s service "{0}" failed: {1}'.format(service_name,str(e)))
  return

def query_docker_service_replicas(endpoint,service_name):
  service_name = service_name.split('_')[1]
  log=logging.getLogger('pk_docker')
  instance = 1
  if pk_config.simulate():
    return
  kubernetes.config.load_kube_config()
  client = kubernetes.client.ExtensionsV1beta1Api()
  try:
    dep = client.read_namespaced_deployment(service_name, "default")
    replicas = dep.spec.replicas
    log.debug('(C) => m_container_count for {0}: {1}'.format(service_name,replicas))
  except Exception as e:
    log.warning('(C) Querying k8s service "{0}" replicas failed: {1}'.format(service_name,str(e)))
  return instance

def query_service_network(endpoint, stack_name, service_name):
  id = None
  log=logging.getLogger('pk_docker')
  #client = docker.DockerClient(base_url=endpoint)
  #full_service_name = stack_name + "_" + service_name
  #if pk_config.simulate():
    #return None
  #service_list = client.services.list()
  #i = 0
  #while i < len(service_list) and service_list[i].name != full_service_name:
    #i += 1
  #if i < len(service_list) and service_list[i].name == full_service_name:
    #if len(service_list[i].attrs.get("Spec").get("TaskTemplate").get("Networks")) == 1:
      #id = service_list[i].attrs.get("Spec").get("TaskTemplate").get("Networks")[0].get("Target")
      #log.debug('Docker service "{0}" in stack "{1}" is connected to network "{2}" with id "{3}".'
		 #.format(service_name, stack_name, client.networks.get(id).name),str(id))
    #else:
      #log.warning('Docker service "{0}" is connected to more than one network.'.format(full_service_name))
  #else:
  log.warning('Docker networking not supported in kubernetes, cannot query')
  return id


def attach_container_to_network(endpoint, container, network_id):
  log=logging.getLogger('pk_docker')
  #client = docker.DockerClient(base_url=endpoint)
  #network = client.networks.get(network_id)
  #if client.containers.get(container).status == "running":
    #network.connect(container)
    #log.info('Container "{0}" is connected to network "{1}".'.format(container, network.name))
  #else:
  log.warning('Docker networking not supported in kubernetes, attach container failed')
  return

def detach_container_from_network(endpoint, container, network_id):
  log=logging.getLogger('pk_docker')
  #client = docker.DockerClient(base_url=client_address, version=client_version)
  #network = client.networks.get(network_id)
  #if client.containers.get(container).status == "running":
    #network.disconnect(container)
    #log.info('Container "{0}" is disconnected from network "{1}".'.format(container, network.name))
  #else:
  log.warning('Docker networking not supported in kubernetes, detach container failed')
  return

down_nodes_stored={}

def remove_node(endpoint,id):
  log=logging.getLogger('pk_docker')
  if pk_config.simulate():
    return
  kubernetes.config.load_kube_config()
  client = kubernetes.client.CoreV1Api()
  try:
    client.delete_node(id, {})
  except Exception:
    log.error('(M) => Removing k8s node failed.')
  return

def down_nodes_cleanup_by_list(stored, actual):
  setStored = { v['ID'] for k,v in stored.items() }
  setActual = { x['ID'] for x in actual }
  missing = { x for x in setStored if x not in setActual }
  for x in missing:
    del stored[x]

def down_nodes_add_from_list(stored, actual):
  for node in actual:
    if 'ID' in node and node['ID'] not in stored:
      stored[node['ID']]=node
      stored[node['ID']]['micado_timestamp']=int(time.time())

def down_nodes_cleanup_by_timeout(endpoint, stored, timeout):
  log=logging.getLogger('pk_docker')
  current_time = int(time.time())
  for id, node in stored.items():
    if node['micado_timestamp']+timeout < current_time:
      log.info('(M) => Node {0} is down for more than {1} seconds, removing.'.format(id,timeout))
      remove_node(endpoint,id)
      del stored[id]

def down_nodes_maintenance(endpoint, down_nodes_timeout = 120):
  log=logging.getLogger('pk_docker')
  down_nodes_actual = query_list_of_nodes(endpoint,status='down')
  down_nodes_cleanup_by_list(down_nodes_stored, down_nodes_actual)
  down_nodes_add_from_list(down_nodes_stored, down_nodes_actual)
  down_nodes_cleanup_by_timeout(endpoint, down_nodes_stored, down_nodes_timeout)
  return
