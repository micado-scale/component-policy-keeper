import logging
import requests
import pk_config

dryrun_id = 'occopus'

def scale_worker_node(endpoint,infra_name,worker_name,replicas):
    log=logging.getLogger('pk_occopus')
    if pk_config.dryrun_get(dryrun_id):
      log.info('(S)   DRYRUN enabled. Skipping...')
      return
    log.info('(S)   => m_node_count: {0}'.format(replicas))
    wscall = '{0}/infrastructures/{1}/scaleto/{2}/{3}'.format(endpoint,infra_name,worker_name,replicas)
    log.debug('-->curl -X POST {0}'.format(wscall))
    response = requests.post(wscall).json()
    log.debug('-->response: {0}'.format(response))
    return

def query_number_of_worker_nodes(endpoint,infra_name,worker_name):
    log=logging.getLogger('pk_occopus')
    instances=1
    if pk_config.dryrun_get(dryrun_id):
      log.info('(C)   DRYRUN enabled. Skipping...')
      return instances
    wscall = '{0}/infrastructures/{1}'.format(endpoint,infra_name)
    log.debug('-->curl -X GET {0}'.format(wscall))
    response = requests.get(wscall).json()
    instances = response.get(worker_name,dict()).get('scaling',dict()).get('target',0)
    log.debug('-->instances: {0}, response: {1}'.format(instances,response))
    return instances

