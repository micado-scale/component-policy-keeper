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
import argparse
import pk_rest
import pk_config

log = None

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
        config = pk_config.get_config()
        dock.scale_docker_service(config['swarm_endpoint'],service_name,int(srv['instances']))

def perform_worker_node_scaling(policy):
  node = policy['scaling']['nodes']
  if 'instances' in node:
    log.debug('(S) Scaling values for worker node: min:{0} max:{1} calculated:{2}'
             .format(node['min'],node['max'],node['instances']))
    node['instances'] = max(min(int(node['instances']),int(node['max'])),int(node['min']))
    config = pk_config.get_config()
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

def load_policy_from_file(policyfile):
  policy = None
  with open(policyfile,'r') as f:
    policy = f.read()
  return policy

def start(policy_yaml):
  global log
  log = logging.getLogger('pk')
  config = pk_config.get_config()
  log.info('Received policy: \n{0}'.format(policy_yaml))
  policy = yaml.safe_load(policy_yaml)
  resolve_queries(policy)
  log.info('(C) Add exporters to prometheus configuration file starts')
  prom.add_exporters_to_prometheus_config(policy,
                                          config['prometheus_config_template'],
                                          config['prometheus_config_target'])
  log.info('(C) Attach prometheus to network of exporters starts')
  prom.attach_prometheus_to_exporters_network(policy,
                                           config['swarm_endpoint'])
  log.info('(C) Add alerts to prometheus, generating rule files starts')
  prom.deploy_alerts_under_prometheus(config['prometheus_rules_directory'],
                                      policy.get('data',dict()).get('alerts'),
                                      policy.get('stack','pk'))
  log.info('(C) Notify prometheus to reload config starts')
  prom.notify_to_reload_config(config['prometheus_endpoint'])
  while not pk_config.get_finish_scaling():
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
      log.exception('Exception occured during policy execution:')
    for x in range(15):
      if pk_config.get_finish_scaling():
        break
      time.sleep(1)
  pk_config.set_finish_scaling(False)

def stop(policy_yaml):
  global log
  log = logging.getLogger('pk')
  config = pk_config.get_config()
  policy = yaml.safe_load(policy_yaml)
  log.info('(C) Remove exporters from prometheus configuration file starts')
  prom.remove_exporters_from_prometheus_config(config['prometheus_config_template'],
                                               config['prometheus_config_target'])
  log.info('(C) Remove alerts from prometheus, deleting rule files starts')
  prom.remove_alerts_under_prometheus(config['prometheus_rules_directory'],
                                      policy.get('data',dict()).get('alerts'),
                                      policy.get('stack','pk'))
  log.info('(C) Notify prometheus to reload config starts')
  prom.notify_to_reload_config(config['prometheus_endpoint'])
  log.info('(C) Detach prometheus from network of exporters starts')
  prom.detach_prometheus_from_exporters_network(policy,
                                                config['swarm_endpoint'])

def perform_policy_keeping(policy_yaml):
  start(policy_yaml)
  stop(policy_yaml)

def pkmain():
  global log
  parser = argparse.ArgumentParser(description='MiCADO component to realise scaling policies')
  parser.add_argument('--cfg',
                      dest='cfg_path',
                      default='./config.yaml',
                      help='path to configuration file')
  parser.add_argument('--policy',
                      dest='cfg_policy',
                      help='specifies the policy to execute')
  parser.add_argument('--srv',
                      action='store_true',
                      dest='cfg_srv',
                      default=False,
                      help='run in service mode')
  args = parser.parse_args()

  #print 'CFG: '+args.cfg_path 
  #print 'POLICY: '+(args.cfg_policy if args.cfg_policy else 'undefined')
  #print 'SRV: '+str(args.cfg_srv) 
 
  #read configuration
  try:
    with open(args.cfg_path,'r') as c:
      pk_config.set_config(yaml.safe_load(c))
  except Exception as e:
    print 'ERROR: Cannot read configuration file "{0}": {1}'.format(args.cfg_path,str(e))
  config = pk_config.get_config()
  #initialise logging facility based on the configuration
  try: 
    logging.config.dictConfig(config['logging'])
    log = logging.getLogger('pk')
  except Exception as e:
    print 'ERROR: Cannot process configuration file "{0}": {1}'.format(args.cfg_path,str(e))

  #read policy file and start periodic policy evaluation in case of command-line mode 
  if not args.cfg_srv:
    if not args.cfg_policy:
      log.error('Policy file must be specified for standalone execution!')
      sys.exit(1)
    try:
      policy_yaml = load_policy_from_file(args.cfg_policy)
      start(policy_yaml)
    except KeyboardInterrupt: 
      log.warning('Keyboard interruption detected! Shutting down...')
      stop(policy_yaml)
    except Exception:
      log.exception('An error occured during policy execution:')
      return

  #launch web service and wait for oncoming requests
  if args.cfg_srv:
    if args.cfg_policy:
      log.warning('Policy file in parameter is unsused, must be defined through the API in service mode!')
    pk_rest.init_service()
    pk_rest.app.run(debug=True,
                    host='0.0.0.0',
                    port=12345)
  
if __name__ == '__main__':
  pkmain()
