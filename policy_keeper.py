#!/usr/bin/env python
import time, sys
import requests
from ruamel import yaml
import json
import handle_docker as dock
import handle_occopus as occo
import handle_prometheus as prom
import jinja2
import logging
import logging.config
import evaluator

config=None
log=None

def resolve_queries(policy):
  if policy['data'].get('queries') and policy['data'].get('constants'):
    for param,query in policy['data']['queries'].iteritems():
      template = jinja2.Template(query)
      newq = template.render(policy['data']['constants'])
      policy['data']['queries'][param]=newq

def perform_service_scaling(policy,service_name):
  for srv in policy['scaling']['services']:
    if 'instances' in srv and srv['name']==service_name:
        log.debug('(S) Scaling values for service "{0}": min:{1} max:{2} calculated:{3}'
		.format(srv['name'],srv['min'],srv['max'],srv['instances']))
        srv['instances'] = max(min(int(srv['instances']),int(srv['max'])),int(srv['min']))
	if policy.get('stack','') not in [None, '']:
          service_name='{0}_{1}'.format(policy['stack'],srv['name'])
        else:
          service_name='{0}'.format(srv['name'])
        dock.scale_docker_service(config['swarm_endpoint'],service_name,int(srv['instances']))

def perform_worker_node_scaling(policy):
  node = policy['scaling']['nodes']
  if 'instances' in node:
    log.debug('(S) Scaling values for worker node: min:{0} max:{1} calculated:{2}'
             .format(node['min'],node['max'],node['instances']))
    node['instances'] = max(min(int(node['instances']),int(node['max'])),int(node['min']))
    occo.scale_occopus_worker_node(
        endpoint=config['occopus_endpoint'],
        infra_name=config['occopus_infra_name'],
        worker_name=config['occopus_worker_name'],
        replicas=node['instances'])

def perform_policy_evaluation_on_a_docker_service(policy,service_name):
   inpvars = dict()
   outvars = ['instances']
   for srv in policy['scaling']['services']:
     if srv['name'] != service_name:
       continue
     inpvars = {}
     if 'instances' not in srv:
       srv['instances']=srv['min']
     for attrname, attrvalue in policy['data']['query_results'].iteritems():
       inpvars[attrname]=attrvalue
     for attrname, attrvalue in policy['data']['constants'].iteritems():
       inpvars[attrname]=attrvalue
     inpvars['instances'] = srv['instances']
     if srv.get('target','') is not None:
       result = evaluator.evaluate(srv.get('target',''), inpvars, outvars)
       srv['instances']=int(result.get('instances',srv['instances']))
     log.info('(P) => instances: {0}'.format(int(srv['instances'])))
   return

def perform_policy_evaluation_on_worker_nodes(policy):
   node = policy['scaling']['nodes']
   inpvars = dict()
   outvars = ['instances']
   if 'instances' not in node:
     node['instances']=node['min']
   for attrname, attrvalue in policy['data']['query_results'].iteritems():
     inpvars[attrname]=attrvalue
   for attrname, attrvalue in policy['data']['constants'].iteritems():
     inpvars[attrname]=attrvalue
   inpvars['instances'] = node['instances']
   if node.get('target','') is not None:
     result = evaluator.evaluate(node.get('target',''), inpvars, outvars)
     node['instances']=int(result.get('instances',node['instances']))
   log.info('(P) => instances: {0}'.format(int(node['instances'])))
   return

def add_exporters_to_prometheus_config(policy):
  log.debug("--> Adding exporters to Prometheus config...")
  for exporter_endpoint in policy.get('data',dict()).get('sources',dict()):
    try:
      log.info('Add exporter "{0}" to Prometheus config.'.format(exporter_endpoint))
      log.warning('Add exporter to Prometheus config is not implemented yet!')
      '''
      TODO: add exporter_ednpoint to the config file of prometheus as targets
      TODO: when each has been added, force prometheus to reload its config
      '''
    except Exception as e:
      log.exception('Policy Keeper')
  log.debug("--> Finished adding exporters to Prometheus config.")
  return

def attach_prometheus_exporters_network(policy):
  log.debug("--> Attaching Prometheus to exporters network...")
  for exporter_endpoint in policy.get('data',dict()).get('sources',dict()):
    try:
      exporter_name=exporter_endpoint.split(':')[0]
      if '.' not in exporter_name:
        log.info('Attach prometheus to network of exporter "{0}".'.format(exporter_endpoint))
        exporter_netid = dock.query_service_network(config['swarm_endpoint'],policy['stack'],exporter_name)
        if exporter_netid:
	  dock.attach_container_to_network(config['swarm_endpoint'], 'prometheus', exporter_netid)
    except Exception as e:
      log.exception('Policy Keeper')
  log.debug("--> Finished attaching Prometheus to exporters network.")

def start(policy):
  log.debug("POLICY:")
  log.debug(policy)
  resolve_queries(policy)
  add_exporters_to_prometheus_config(policy)
  attach_prometheus_exporters_network(policy)
  while True:
    try:
      log.info('(Q) Query evaluation for nodes starts')
      queries = prom.evaluate_data_queries_for_nodes(config['prometheus_endpoint'],policy)
      if queries:
        for attrname, attrvalue in queries.iteritems():
          log.info('(Q) => "{0}" is "{1}".'.format(attrname,attrvalue))
      
        log.info('(P) Policy evaluation for nodes starts')
        perform_policy_evaluation_on_worker_nodes(policy)
        log.info('(S) Scaling of nodes starts')
        perform_worker_node_scaling(policy)
      else:
        log.info('(Q) No query evaluation performed for nodes, skipping policy evaluation')

      for oneservice in policy.get('scaling',dict()).get('services',dict()):
        service_name=oneservice.get('name')
        log.info('(Q) Query evaluation for service "{0}" starts'.format(service_name))
        queries = prom.evaluate_data_queries_for_a_service(config['prometheus_endpoint'],policy,service_name)
        if queries:
          for attrname, attrvalue in queries.iteritems():
            log.info('(Q) => "{0}" is "{1}".'.format(attrname,attrvalue))
          log.info('(P) Policy evaluation for service "{0}" starts'.format(service_name))
          perform_policy_evaluation_on_a_docker_service(policy,service_name)
          log.info('(S) Scaling of service "{0}" starts'.format(service_name))
          perform_service_scaling(policy,service_name)
        else:
          log.info('(Q) No query evaluation performed for service "{0}", skipping policy evaluation'
                   .format(service_name))

    except Exception as e:
      log.exception('Exception occured in Policy Keeper:')
    time.sleep(15)

def stop():
  log.error("Not implemented yet")

with open('config.yaml','r') as c:
  config=yaml.safe_load(c)
logging.config.dictConfig(config['logging'])
log = logging.getLogger('pk')

if len(sys.argv)!=2:
  log.error('Argument must be a policy file."')
  sys.exit(1)

def load_policy_from_file(policyfile):
  policy = None
  with open(policyfile,'r') as f:
    policy = yaml.safe_load(f)
  return policy

policy = load_policy_from_file(sys.argv[1])
start(policy)



