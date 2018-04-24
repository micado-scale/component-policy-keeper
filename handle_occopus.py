import logging
import requests

def scale_occopus_worker_node(endpoint,infra_name,worker_name,replicas):
    log=logging.getLogger('pk_occopus')
    log.info('(S) => replicas: {0}'.format(replicas))
    wscall = '{0}/infrastructures/{1}/scaleto/{2}/{3}'.format(endpoint,infra_name,worker_name,replicas)
    log.debug('-->curl -X POST {0}'.format(wscall))
    response = requests.post(wscall).json()
    log.debug('-->response: {0}'.format(response))
    return


