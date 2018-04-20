import logging
import requests

def scale_occopus_worker_node(endpoint,infra_name,worker_name,replicas):
    log=logging.getLogger('pk_occopus')
    log.info('Scaling occopus worker node "{0}" to {1} replicas.'.format(worker_name,replicas))
    wscall = '{0}/infrastructures/{1}/scaleto/{2}/{3}'.format(endpoint,infra_name,worker_name,replicas)
    log.info('-->curl -X POST {0}'.format(wscall))
    response = requests.post(wscall).json()
    log.info('-->response: {0}'.format(response))
    return


