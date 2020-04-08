import logging
import requests
import pk_config

dryrun_id = 'occopus'

CONFIG_ENDPOINT = 'occopus_endpoint'
CONFIG_INFRA_NAME = 'occopus_infra_name'

def scale_worker_node(config,scaling_info_list):
    log=logging.getLogger('pk_occopus')
    if pk_config.dryrun_get(dryrun_id):
      log.info('(S)   DRYRUN enabled. Skipping...')
      return
    endpoint, infra_name = config[CONFIG_ENDPOINT], config[CONFIG_INFRA_NAME]
    for info in scaling_info_list:
      worker_name, replicas = info.get('node_name'), info.get('replicas')
      log.info('(S) {0}  => m_node_count: {1}'.format(worker_name, replicas))
      wscall = '{0}/infrastructures/{1}/scaleto/{2}/{3}'.format(endpoint,infra_name,worker_name,replicas)
      log.debug('-->curl -X POST {0}'.format(wscall))
      response = requests.post(wscall).json()
      log.debug('-->response: {0}'.format(response))
    return

def query_number_of_worker_nodes(config,worker_name):
    log=logging.getLogger('pk_occopus')
    instances=1
    if pk_config.dryrun_get(dryrun_id):
      log.info('(C)   DRYRUN enabled. Skipping...')
      return instances
    endpoint, infra_name = config[CONFIG_ENDPOINT], config[CONFIG_INFRA_NAME]
    wscall = '{0}/infrastructures/{1}'.format(endpoint,infra_name)
    log.debug('-->curl -X GET {0}'.format(wscall))
    response = requests.get(wscall).json()
    instances = response.get(worker_name,dict()).get('scaling',dict()).get('target',0)
    log.debug('-->instances: {0}, response: {1}'.format(instances,response))
    return instances

def drop_worker_node(config,scaling_info_list):
    log=logging.getLogger('pk_occopus')
    if pk_config.dryrun_get(dryrun_id):
      log.info('(S)   DRYRUN enabled. Skipping...')
      return
    endpoint, infra_name = config[CONFIG_ENDPOINT], config[CONFIG_INFRA_NAME]
    for info in scaling_info_list:
      worker_name, replicas = info.get('node_name'), info.get('replicas')
      for replica in replicas:
        log.info('(S) {0}  => node drop: {1}'.format(worker_name, replica))
        wscall = '{0}/infrastructures/{1}/scaledown/{2}/{3}'.format(endpoint,infra_name,worker_name,replica)
        log.debug('-->curl -X POST {0}'.format(wscall))
        response = requests.post(wscall).json()
        log.debug('-->response: {0}'.format(response))
    return

