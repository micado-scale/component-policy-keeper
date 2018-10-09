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

def resolve_queries(policy_yaml):
  stack = dict()
  stack['stack'] = yaml.safe_load(policy_yaml).get('stack','undefined_stack_name')
  env = jinja2.Environment(undefined=jinja2.DebugUndefined)
  template = env.from_string(policy_yaml)
  policy_yaml = template.render(stack)

  values = yaml.safe_load(policy_yaml).get('data',dict()).get('constants',dict())
  log.info('values: {0}'.format(values))
  template = jinja2.Template(policy_yaml)
  return template.render(values)

def get_full_service_name(policy, service_name):
  if policy.get('stack','') not in [None, '']:
    full_service_name='{0}_{1}'.format(policy['stack'],service_name)
  else:
    full_service_name='{0}'.format(service_name)
  return full_service_name

def perform_service_scaling(policy,service_name):
  for srv in policy['scaling']['services']:
    if 'm_container_count' in srv.get('outputs',dict()) and srv['name']==service_name:
        log.debug('(S) Scaling values for service "{0}": min:{1} max:{2} calculated:{3}'
		.format(srv['name'],srv['min_instances'],srv['max_instances'],srv['outputs']['m_container_count']))
        containercount = max(min(int(srv['outputs']['m_container_count']),int(srv['max_instances'])),int(srv['min_instances']))
        service_name = get_full_service_name(policy, srv['name'])
        config = pk_config.config()
        dock.scale_docker_service(config['swarm_endpoint'],service_name,containercount)

def perform_worker_node_scaling(policy):
  node = policy['scaling']['nodes']
  if 'm_node_count' in node.get('outputs',dict()):
    log.debug('(S) Scaling values for worker node: min:{0} max:{1} calculated:{2}'
             .format(node['min_instances'],node['max_instances'],node['outputs']['m_node_count']))
    nodecount = max(min(int(node['outputs']['m_node_count']),int(node['max_instances'])),int(node['min_instances']))
    config = pk_config.config()
    occo.scale_worker_node(
        endpoint=config['occopus_endpoint'],
        infra_name=config['occopus_infra_name'],
        worker_name=config['occopus_worker_name'],
        replicas=nodecount)

def perform_policy_evaluation_on_a_docker_service(policy,service_name):
   outvars = ['m_container_count','m_userdata']
   for srv in policy['scaling']['services']:
     if srv['name'] != service_name:
       continue
     inpvars = srv['inputs']
     inpvars['m_userdata'] = policy['scaling'].get('userdata',None)
     for attrname, attrvalue in policy.get('data',dict()).get('query_results',dict()).iteritems():
       inpvars[attrname]=attrvalue
     for attrname, attrvalue in policy.get('data',dict()).get('alert_results',dict()).iteritems():
       inpvars[attrname]=attrvalue
     for attrname, attrvalue in policy.get('data',dict()).get('constants',dict()).iteritems():
       inpvars[attrname]=attrvalue
     if srv.get('scaling_rule','') is not None:
       result = evaluator.evaluate(srv.get('scaling_rule',''), inpvars, outvars)
       if 'outputs' not in srv:
         srv['outputs']={}
       srv['outputs']['m_container_count']=int(result.get('m_container_count',srv['inputs']['m_container_count']))
       policy['scaling']['userdata']=result.get('m_userdata',None)
     log.info('(P) => m_container_count: {0}'.format(int(srv.get('outputs',dict()).get('m_container_count',0))))
   return

def perform_policy_evaluation_on_worker_nodes(policy):
   node = policy['scaling']['nodes']
   inpvars = node['inputs']
   outvars = ['m_node_count','m_userdata']
   inpvars['m_userdata'] = policy['scaling'].get('userdata',None)
   for attrname, attrvalue in policy.get('data',dict()).get('query_results',dict()).iteritems():
     inpvars[attrname]=attrvalue
   for attrname, attrvalue in policy.get('data',dict()).get('alert_results',dict()).iteritems():
     inpvars[attrname]=attrvalue
   for attrname, attrvalue in policy.get('data',dict()).get('constants',dict()).iteritems():
     inpvars[attrname]=attrvalue
   if node.get('scaling_rule','') is not None:
     result = evaluator.evaluate(node.get('scaling_rule',''), inpvars, outvars)
     if 'outputs' not in node:
       node['outputs']={}
     node['outputs']['m_node_count']=int(result.get('m_node_count',node['inputs']['m_node_count']))
     policy['scaling']['userdata']=result.get('m_userdata',None)
   log.info('(P) => m_node_count: {0}'.format(int(node.get('outputs',dict()).get('m_node_count',0))))
   return False if node['outputs']['m_node_count'] == node['inputs']['m_node_count'] else True 

def load_policy_from_file(policyfile):
  policy = None
  with open(policyfile,'r') as f:
    policy = f.read()
  return policy

def set_worker_node_instance_number(policy,instances):
  policy.setdefault('scaling',dict())
  policy['scaling'].setdefault('nodes',dict())
  policy['scaling']['nodes'].setdefault('outputs',dict())
  policy['scaling']['nodes']['outputs']['m_node_count']=instances
  return

def set_docker_service_instance_number(policy,service_name,instances):
  for theservice in policy.get('scaling',dict()).get('services',dict()):
    if service_name == theservice.get('name',''):
      theservice.setdefault('outputs',dict())
      theservice['outputs']['m_container_count']=instances
  return

def prepare_session(policy_yaml):
  global log
  log = logging.getLogger('pk')
  config = pk_config.config()
  log.info('Received policy: \n{0}'.format(policy_yaml))
  policy_yaml = resolve_queries(policy_yaml)
  log.info('Resolved policy: \n{0}'.format(policy_yaml))
  policy = yaml.safe_load(policy_yaml)
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
  log.info('(C) Querying number of target nodes from Occopus starts')
  instances = occo.query_number_of_worker_nodes(
                   endpoint=config['occopus_endpoint'],
                   infra_name=config['occopus_infra_name'],
                   worker_name=config['occopus_worker_name'])
  log.info('(C) Setting m_node_count to {0}'.format(instances))
  set_worker_node_instance_number(policy,instances)
  log.info('(C) Querying number of service replicas from Swarm starts')
  for theservice in policy.get('scaling',dict()).get('services',dict()):
    service_name = theservice.get('name','')
    full_service_name = get_full_service_name(policy, service_name)
    instances = dock.query_docker_service_replicas(config['swarm_endpoint'],full_service_name)
    log.info('(C) Setting m_container_count for {0} to {1}'.format(service_name, instances))
    set_docker_service_instance_number(policy,service_name,instances)
  return policy

def add_query_results_and_alerts_to_nodes(policy, results):
  queries, alerts = dict(), dict()
  policy['data']['query_results']={}
  policy['data']['alert_results']={}
  scaling_rule_str = policy.get('scaling',dict()).get('nodes',dict()).get('scaling_rule','')
  for attrname, attrvalue in results.get('data',dict()).get('queries',dict()).iteritems():
    if scaling_rule_str is not None and scaling_rule_str.find(attrname) != -1:
      queries[attrname]=attrvalue
      policy['data']['query_results'][attrname]=attrvalue
  fired_alerts = dict()
  for item in results.get('data',dict()).get('alerts',dict()):
    fired_alerts[item['alert']]=True
  for item in policy.get('data',dict()).get('alerts',dict()):
    attrname = item['alert']
    if scaling_rule_str is not None and scaling_rule_str.find(attrname) != -1:
      if attrname in fired_alerts:
        policy['data']['alert_results'][attrname]=True
        alerts[attrname]=True
      else:
        policy['data']['alert_results'][attrname]=False
        alerts[attrname]=False
  return queries, alerts
  
def add_query_results_and_alerts_to_service(policy, results, servicename):
  queries, alerts = dict(), dict()
  policy['data']['query_results']={}
  policy['data']['alert_results']={}
  all_services = policy.get('scaling',dict()).get('services',dict())
  target_service = [ srv for srv in all_services if srv.get('name','')==servicename ]
  scaling_rule_str = target_service[0].get('scaling_rule','') if target_service else ''
  for attrname,attrvalue in results.get('data',dict()).get('queries',dict()).iteritems():
    if scaling_rule_str is not None and scaling_rule_str.find(attrname) != -1:
      queries[attrname]=attrvalue
      policy['data']['query_results'][attrname]=attrvalue
  fired_alerts = dict()
  for item in results.get('data',dict()).get('alerts',dict()):
    fired_alerts[item['alert']]=True
  for item in policy.get('data',dict()).get('alerts',dict()):
    attrname = item['alert']
    if scaling_rule_str is not None and scaling_rule_str.find(attrname) != -1:
      if attrname in fired_alerts:
        policy['data']['alert_results'][attrname]=True
        alerts[attrname]=True
      else:
        policy['data']['alert_results'][attrname]=False
        alerts[attrname]=False
  return queries, alerts

def collect_inputs_for_nodes(policy):
  inputs={}
  node = policy.get('scaling',dict()).get('nodes',dict())
  config = pk_config.config()
  inputs['m_nodes']=dock.query_list_of_nodes(config['swarm_endpoint'])
  mnc = node.get('outputs',dict()).get('m_node_count',None)
  inputs['m_node_count'] = max(min(int(mnc),int(node['max_instances'])),int(node['min_instances'])) if mnc else int(node['min_instances'])
  
  prev_node_count = node.get('inputs',dict()).get('m_node_count',None)
  prev_nodes = node.get('inputs',dict()).get('m_nodes',None)
  if prev_node_count and prev_nodes:
    if prev_node_count == len(prev_nodes):
      if inputs['m_node_count']==len(inputs['m_nodes']):
        inputs['m_time_when_node_count_changed'] = node.get('inputs',dict()).get('m_time_when_node_count_changed',0)
      else:
        inputs['m_time_when_node_count_changed'] = 0
    else:
      if inputs['m_node_count']==len(inputs['m_nodes']):
        inputs['m_time_when_node_count_changed'] = int(time.time())
      else:
        inputs['m_time_when_node_count_changed'] = 0
  else:
    inputs['m_time_when_node_count_changed'] = int(time.time())
  if inputs['m_time_when_node_count_changed'] == 0:
    inputs['m_time_since_node_count_changed'] = 0
  else:
    inputs['m_time_since_node_count_changed'] = int(time.time())-inputs['m_time_when_node_count_changed']
 
  inputs['m_userdata']=policy.get('scaling',dict()).get('userdata',None)
  return inputs

def set_policy_inputs_for_nodes(policy,inputs):
  policy['scaling']['nodes']['inputs']=inputs

def collect_inputs_for_containers(policy,service_name):
  inputs={}
  config = pk_config.config()
  node = policy.get('scaling',dict()).get('nodes',dict())
  inputs['m_nodes']=dock.query_list_of_nodes(config['swarm_endpoint'])
  mnc = node.get('outputs',dict()).get('m_node_count',None)
  inputs['m_node_count'] = max(min(int(mnc),int(node['max_instances'])),int(node['min_instances'])) if mnc else int(node['min_instances'])
  for theservice in policy.get('scaling',dict()).get('services',dict()):
    if service_name == theservice.get('name',''):
      mcc = theservice.get('outputs',dict()).get('m_container_count',None)
      inputs['m_container_count'] = max(min(int(mcc),int(theservice['max_instances'])),int(theservice['min_instances']))\
            if mcc else int(theservice['min_instances'])
  inputs['m_userdata']=policy.get('scaling',dict()).get('userdata',None)
  return inputs

def set_policy_inputs_for_containers(policy,service_name,inputs):
  for theservice in policy.get('scaling',dict()).get('services',dict()):
    if service_name == theservice.get('name',''):
      theservice['inputs']=inputs

def perform_one_session(policy, results = None):
  global log
  log = logging.getLogger('pk')
  config = pk_config.config()
  log.info('--- session starts ---')
  log.info('(M) Maintaining docker nodes starts')
  dock.down_nodes_maintenance(config['swarm_endpoint'],config['docker_node_unreachable_timeout'])
  log.info('(I) Collecting inputs for nodes starts')
  inputs = collect_inputs_for_nodes(policy)
  set_policy_inputs_for_nodes(policy,inputs)
  for x in inputs.keys():
    log.info('(I) => "{0}": {1}'.format(x,inputs[x]))
  log.info('(Q) Evaluating queries and alerts for nodes starts')
  if results:
    queries, alerts = add_query_results_and_alerts_to_nodes(policy, results)
  else:
    queries, alerts = prom.evaluate_data_queries_and_alerts_for_nodes(config['prometheus_endpoint'],policy)
  for attrname, attrvalue in queries.iteritems():
    log.info('(Q) => "{0}" is "{1}".'.format(attrname,attrvalue))
  for attrname, attrvalue in alerts.iteritems():
    log.info('(A) => "{0}" is "{1}".'.format(attrname,attrvalue))
  log.info('(P) Policy evaluation for nodes starts')
  if perform_policy_evaluation_on_worker_nodes(policy):
    log.info('(S) Scaling of nodes starts')
    perform_worker_node_scaling(policy)
  for attrname, attrvalue in alerts.iteritems():
    prom.alerts_remove(attrname)
  
  for oneservice in policy.get('scaling',dict()).get('services',dict()):
    service_name=oneservice.get('name')
    log.info('(I) Collecting inputs for service "{0}" starts'.format(service_name))
    inputs = collect_inputs_for_containers(policy,service_name)
    set_policy_inputs_for_containers(policy,service_name,inputs)
    for x in inputs.keys():
      log.info('(I) => "{0}": {1}'.format(x,inputs[x]))
    log.info('(Q) Evaluating queries and alerts for service "{0}" starts'.format(service_name))
    if results:
      queries, alerts = add_query_results_and_alerts_to_service(policy, results, service_name)
    else:
      queries, alerts = prom.evaluate_data_queries_and_alerts_for_a_service(
                             config['prometheus_endpoint'],policy,service_name)
    for attrname, attrvalue in queries.iteritems():
      log.info('(Q) => "{0}" is "{1}".'.format(attrname,attrvalue))
    for attrname, attrvalue in alerts.iteritems():
      log.info('(A) => "{0}" is "{1}".'.format(attrname,attrvalue))
    log.info('(P) Policy evaluation for service "{0}" starts'.format(service_name))
    perform_policy_evaluation_on_a_docker_service(policy,service_name)
    log.info('(S) Scaling of service "{0}" starts'.format(service_name))
    perform_service_scaling(policy,service_name)
    for attrname, attrvalue in alerts.iteritems():
      prom.alerts_remove(attrname)
 
  log.info('--- session finished ---')
  return

def start(policy_yaml):
  global log
  log = logging.getLogger('pk')
  policy = prepare_session(policy_yaml)
  pk_config.finish_scaling(False)
  while not pk_config.finish_scaling():
    try:
      perform_one_session(policy)
    except Exception as e:
      log.exception('Exception occured during policy execution:')
    for x in range(15):
      if pk_config.finish_scaling():
        break
      time.sleep(1)

def stop(policy_yaml):
  global log
  log = logging.getLogger('pk')
  config = pk_config.config()
  policy = yaml.safe_load(policy_yaml)
  log.info('(C) Remove exporters from prometheus configuration file starts')
  prom.remove_exporters_from_prometheus_config(config['prometheus_config_template'],
                                               config['prometheus_config_target'])
  log.info('(C) Remove alerts from prometheus, deleting rule files starts')
  prom.remove_alerts_under_prometheus(config['prometheus_rules_directory'],
                                      policy.get('data',dict()).get('alerts',dict()),
                                      policy.get('stack','pk'))
  log.info('(C) Notify prometheus to reload config starts')
  prom.notify_to_reload_config(config['prometheus_endpoint'])
  log.info('(C) Detach prometheus from network of exporters starts')
  prom.detach_prometheus_from_exporters_network(policy,
                                                config['swarm_endpoint'])

def perform_policy_keeping(policy_yaml):
  try:
    start(policy_yaml)
  except Exception:
    log.exception('Internal exception during policy execution:')
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
  parser.add_argument('--simulate',
                      action='store_true',
                      dest='cfg_simulate',
                      default=False,
                      help='ommit manipulating surrounding components')
  parser.add_argument('--host',
                      type=str,
                      default='127.0.0.1',
                      help='host to bind service to')
  parser.add_argument('--port',
                      type=int,
                      default='12345',
                      help='port to bind service to')
  args = parser.parse_args()
  #read configuration
  try:
    with open(args.cfg_path,'r') as c:
      pk_config.config(yaml.safe_load(c))
  except Exception as e:
    print 'ERROR: Cannot read configuration file "{0}": {1}'.format(args.cfg_path,str(e))
  config = pk_config.config()
  #initialise logging facility based on the configuration
  try: 
    logging.config.dictConfig(config['logging'])
    log = logging.getLogger('pk')
  except Exception as e:
    print 'ERROR: Cannot process configuration file "{0}": {1}'.format(args.cfg_path,str(e))
  #set simulate mode
  pk_config.simulate(args.cfg_simulate)
  if args.cfg_simulate:
    log.warning('SIMULATION mode is active! No changes will be performed.')
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
                    host=args.host,
                    port=args.port)
  
if __name__ == '__main__':
  pkmain()
