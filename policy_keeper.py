#!/usr/bin/env python
import time, sys
import requests
from ruamel import yaml
import json
import handle_docker as dock
import handle_prometheus as prom
import jinja2
import logging
import logging.config
import evaluator

config=None
log=None

def resolve_queries(policy):
  for param,query in policy['data']['queries'].iteritems():
    template = jinja2.Template(query)
    newq = template.render(policy['data']['constants'])
    policy['data']['queries'][param]=newq

def perform_service_scaling(policy):
  for srv in policy['scaling']['services']:
    if 'instances' in srv:
        log.info('Scaling values for service "{0}": min:{1} max:{2} calculated:{3}'
		.format(srv['name'],srv['min'],srv['max'],srv['instances']))
        srv['instances'] = max(min(int(srv['instances']),int(srv['max'])),int(srv['min']))
	service_name='{0}_{1}'.format(policy['stack'],srv['name'])
        dock.scale_docker_service(config['swarm_endpoint'],service_name,int(srv['instances']))

def perform_policy_evaluation(policy):
   inpvars = dict()
   outvars = ['instances']
   for srv in policy['scaling']['services']:
     inpvars = {}
     if 'instances' not in srv:
       srv['instances']=srv['min']
     for attrname, attrvalue in policy['data']['query_results'].iteritems():
       inpvars[attrname]=attrvalue
     for attrname, attrvalue in policy['data']['constants'].iteritems():
       inpvars[attrname]=attrvalue
     inpvars['instances'] = srv['instances']
     result = evaluator.evaluate(srv['target'], inpvars, outvars)
     srv['instances']=int(result.get('instances',srv['instances']))
     log.info('Outcome of policy evaluation:')
     log.info('-- instances = {0}'.format(int(srv['instances'])))
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
      prom.evaluate_data_queries(config['prometheus_endpoint'],policy)
      log.info('Outcome of evaluating queries:')
      for attrname, attrvalue in policy['data']['query_results'].iteritems():
        log.info('-- "{0}" is "{1}".'.format(attrname,attrvalue))
      perform_policy_evaluation(policy)
      perform_service_scaling(policy)
    except Exception as e:
      log.exception('Policy Keeper')
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

with open(sys.argv[1],'r') as f:
  start(yaml.safe_load(f))



