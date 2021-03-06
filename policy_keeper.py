#!/usr/bin/env python
import time, sys
import requests
from ruamel import yaml
import json
import shutil
import handle_k8s as k8s
import handle_occopus as occo
import handle_terraform as terra
import handle_prometheus as prom
import handle_optimizer as optim
import jinja2
import logging
import logging.config
import evaluator
import argparse
import pk_rest
import pk_config
from pk_helper import *

log = None

def resolve_queries(policy_yaml):
  stack = dict()
  stack['stack'] = yaml.safe_load(policy_yaml).get('stack','undefined_stack_name')
  env = jinja2.Environment(undefined=jinja2.DebugUndefined)
  template = env.from_string(policy_yaml.decode())
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
        k8s.scale_k8s_deploy(config['k8s_endpoint'],service_name,containercount)

def get_node_scaling(node):
  m_node_count = node.get('outputs',dict()).get('m_node_count')
  nodes_to_drop_list = node.get('outputs',dict()).get('m_nodes_todrop',list())
  cloud = get_cloud_orchestrator(node)
  scaling_info = {'node_name': node['name']}
  if nodes_to_drop_list:
    for nodetodrop in nodes_to_drop_list:
      if m_node_count>node['min_instances']:        
        # Collect the nodetodrop info
        log.debug('(S) Plan to drop node {0}...'.format(nodetodrop))
        scaling_info.setdefault('replicas', []).append(nodetodrop)
        m_node_count-=1
    node['outputs']['m_node_count']=m_node_count

    # Return the nodetodrop info along with the appropriate handler method
    return cloud.drop_worker_node, scaling_info
  elif 'm_node_count' in node.get('outputs',dict()):
    nodecount,nmin,nmax = limit_instances(
        node['outputs'].get('m_node_count'),
        node.get('min_instances'),
        node.get('max_instances'))
    log.debug('(S) Scaling values for {0}: min:{1} max:{2} calculated:{3} corrected:{4}'
             .format(node['name'],nmin,nmax,node['outputs'].get('m_node_count',None),nodecount))
    scaling_info['replicas'] = nodecount

    # Return the nodecount info along with the appropriate handler method
    return cloud.scale_worker_node, scaling_info
  return None, None

def perform_policy_evaluation_on_a_k8s_deploy(policy,service_name):
   outvars = ['m_container_count','m_userdata']
   for srv in policy['scaling']['services']:
     if srv['name'] != service_name:
       continue
     inpvars = srv['inputs']
     inpvars['m_userdata'] = policy['scaling'].get('userdata',None)
     for attrname, attrvalue in policy.get('data',dict()).get('query_results',dict()).items():
       inpvars[attrname]=attrvalue
     for attrname, attrvalue in policy.get('data',dict()).get('alert_results',dict()).items():
       inpvars[attrname]=attrvalue
     for attrname, attrvalue in policy.get('data',dict()).get('constants',dict()).items():
       inpvars[attrname]=attrvalue
     inpvars['requests']=requests
     if srv.get('scaling_rule','')!='':
       result = evaluator.evaluate(srv.get('scaling_rule',''), inpvars, outvars)
       if 'outputs' not in srv:
         srv['outputs']={}
       srv['outputs']['m_container_count']=int(result.get('m_container_count',srv['inputs']['m_container_count']))
       policy['scaling']['userdata']=result.get('m_userdata',None)
     log.info('(P)   => m_container_count: {0}'.format(int(srv.get('outputs',dict()).get('m_container_count',0))))
   return

def perform_policy_evaluation_on_worker_nodes(policy, node):
   inpvars = node['inputs']
   outvars = ['m_node_count','m_userdata','m_nodes_todrop']
   inpvars['m_userdata'] = policy['scaling'].get('userdata',None)
   for attrname, attrvalue in policy.get('data',dict()).get('query_results',dict()).items():
     inpvars[attrname]=attrvalue
   for attrname, attrvalue in policy.get('data',dict()).get('alert_results',dict()).items():
     inpvars[attrname]=attrvalue
   for attrname, attrvalue in policy.get('data',dict()).get('constants',dict()).items():
     inpvars[attrname]=attrvalue
   inpvars['m_opt_advice']=optim.calling_rest_api_advice
   inpvars['requests']=requests
   if node.get('scaling_rule','')!='':
     result = evaluator.evaluate(node.get('scaling_rule',''), inpvars, outvars)
     if 'outputs' not in node:
       node['outputs']={}

     nodes_to_drop_list=result.get('m_nodes_todrop',[])
     if nodes_to_drop_list:
       node['outputs']['m_nodes_todrop']=nodes_to_drop_list
       node['outputs']['m_node_count']=node['inputs']['m_node_count']
     else:
       node['outputs']['m_nodes_todrop']=[]
       node['outputs']['m_node_count']=int(result.get('m_node_count',node['inputs']['m_node_count']))
     policy['scaling']['userdata']=result.get('m_userdata',None)
   if node['outputs'].get('m_nodes_todrop'):
     log.info('(P) => m_nodes_todrop for {0}: {1}'.format(node['name'], node['outputs']['m_nodes_todrop']))
   else:
     log.info('(P) => m_node_count for {0}: {1}'.format(node['name'], int(node.get('outputs',dict()).get('m_node_count',0))))
   return

def load_policy_from_file(policyfile):
  policy = None
  with open(policyfile,'r') as f:
    policy = f.read()
  return policy

# Get the correct handler to use for each node operation
# This info needs to be set by the PKadaptor
def get_cloud_orchestrator(node):
  if node.get('orchestrator', '').lower() == 'terraform':
    return terra
  else:
    return occo

def set_worker_node_instance_number(node,instances):
  node.setdefault('outputs',dict())
  node['outputs']['m_node_count']=instances
  return

def set_k8s_instance_number(policy,service_name,instances):
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
  #Set dryrun flags
  log.info('(C) Initializing dryrun settings from policy starts')
  pk_config.dryrun_set()
  dryrun =  policy.get('data',dict()).get('constants',dict()).get('m_dryrun',None)
  if type(dryrun) == list:
    for comp in dryrun:
      if comp in pk_config.var_dryrun_components:
        pk_config.dryrun_set(comp,True)
  log.info('(C) Enable dryrun for the following components: {0}'.format(pk_config.dryrun_get()))
  #Initialize Prometheus
  log.info('(C) Add exporters to prometheus configuration file starts')
  config_tpl = config['prometheus_config_template']
  config_target = config['prometheus_config_target']
  prom.add_exporters_to_prometheus_config(policy, config_tpl, config_target)
  log.info('(C) Add alerts to prometheus, generating rule files starts')
  prom.deploy_alerts_under_prometheus(config['prometheus_rules_directory'],
                                      policy.get('data',dict()).get('alerts'),
                                      policy.get('stack','pk'))
  log.info('(C) Notify prometheus to reload config starts')
  prom.notify_to_reload_config(config['prometheus_endpoint'])
  #Initialise nodes through Occopus
  log.info('(C) Querying number of target nodes from Cloud Orchestrator starts')
  #policy.setdefault('scaling', dict())["cloud_orchestrator"] = get
  for onenode in policy.get('scaling',dict()).get('nodes',[]):
    cloud = get_cloud_orchestrator(onenode)
    instances = cloud.query_number_of_worker_nodes(
                    config,
                    worker_name=onenode['name'])
    log.info('(C) Setting m_node_count for {} to {}'.format(onenode['name'], instances))
    set_worker_node_instance_number(onenode,instances)
  #Initialise service through K8S
  log.info('(C) Querying number of service replicas from K8s starts')
  for theservice in policy.get('scaling',dict()).get('services',[]):
    service_name = theservice.get('name','')
    full_service_name = get_full_service_name(policy, service_name)
    instances = k8s.query_k8s_replicas(config['k8s_endpoint'],full_service_name)
    log.info('(C)   Setting m_container_count for {0} to {1}'.format(service_name, instances))
    set_k8s_instance_number(policy,service_name,instances)
  #Initialise Optimizer
  log.info('(O) Scanning the optimizer parameters starts...')
  optim.collect_init_params_and_variables(policy)
  log.info('(O) Initializing optimizer starts...')
  optim.calling_rest_api_init() 
  return policy

def add_query_results_and_alerts_to_nodes(policy, results, node):
  queries, alerts = dict(), dict()
  policy['data']['query_results']={}
  policy['data']['alert_results']={}
  scaling_rule_str = node.get('scaling_rule','')
  for attrname, attrvalue in results.get('data',dict()).get('queries',dict()).items():
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
  for attrname,attrvalue in results.get('data',dict()).get('queries',dict()).items():
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

def collect_inputs_for_nodes(policy, node):
  inputs={}
  config = pk_config.config()
  inputs['m_nodes']=k8s.query_list_of_nodes(config['k8s_endpoint'], node['name'])
  inputs['m_node_count'],_,_ = limit_instances(
    node.get('outputs',dict()).get('m_node_count'),
    node.get('min_instances'),
    node.get('max_instances'))
  inputs['m_nodes_todrop']=[]

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

def set_policy_inputs_for_nodes(policy,inputs,node):
  node['inputs']=inputs

def collect_inputs_for_containers(policy,service_name):
  inputs={}
  config = pk_config.config()
  nodes = policy.get('scaling',dict()).get('nodes',[])
  inputs['m_nodes'] = []
  mnc, mini, maxi = 0, 0, 0
  
  for theservice in policy.get('scaling',dict()).get('services',[]):

    if service_name == theservice.get('name',''):
      for node in nodes:
        if not theservice.get('hosts') or node['name'] in theservice.get('hosts', []):
          inputs['m_nodes']+=k8s.query_list_of_nodes(config['k8s_endpoint'], node['name'])
          mnc,mini,maxi = limit_instances(node.get('outputs',dict()).get('m_node_count'),
                                          node.get('min_instances'),
                                          node.get('max_instances'))
      inputs['m_node_count'] = mnc
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
  log.info('(M) Maintaining worker nodes starts')
  k8s.down_nodes_maintenance(config['k8s_endpoint'],config['docker_node_unreachable_timeout'])
  nodes_to_scale = dict()

  # Nodes loop
  for onenode in policy.get('scaling',dict()).get('nodes',[]):
    node_name = onenode.get('name')
    log.info('(I) Collecting inputs for node {} starts'.format(node_name))
    inputs = collect_inputs_for_nodes(policy, onenode)
    set_policy_inputs_for_nodes(policy,inputs,onenode)
    for x in list(inputs.keys()):
      log.info('(I)   => "{0}": {1}'.format(x,inputs[x]))
    log.info('(Q) Evaluating queries and alerts for node {} starts'.format(node_name))
    if results:
      queries, alerts = add_query_results_and_alerts_to_nodes(policy, results, onenode)
    else:
      queries, alerts = prom.evaluate_data_queries_and_alerts_for_nodes(config['prometheus_endpoint'],policy, onenode)
    for attrname, attrvalue in queries.items():
      log.info('(Q)   => "{0}" is "{1}".'.format(attrname,attrvalue))
    for attrname, attrvalue in alerts.items():
      log.info('(A)   => "{0}" is "{1}".'.format(attrname,attrvalue))

    if 'm_opt_advice' in onenode.get('scaling_rule',''):
      log.info('(O) Creating sample for the optimizer starts')
      sample = optim.generate_sample(queries,inputs)
      log.info('(O) Sending sample for the optimizer starts')
      optim.calling_rest_api_sample(sample)

    log.info('(P) Policy evaluation for nodes starts')
    perform_policy_evaluation_on_worker_nodes(policy, onenode)
    log.info('(S) Scaling of nodes starts')

    # First, collect orchestrator handler method and info for each node
    scaling_method, scaling_info = get_node_scaling(onenode)
    if scaling_method and scaling_info:
      nodes_to_scale.setdefault(scaling_method, []).append(scaling_info)
    for attrname, attrvalue in alerts.items():
      prom.alerts_remove(attrname)

  # Then, scale nodes using the correct orchestrator and scaling info    
  for handler_method, scaling_info in nodes_to_scale.items():
    handler_method(config, scaling_info)

  # Containers loop
  for oneservice in policy.get('scaling',dict()).get('services',[]):
    service_name=oneservice.get('name')
    log.info('(I) Collecting inputs for service "{0}" starts'.format(service_name))
    inputs = collect_inputs_for_containers(policy,service_name)
    set_policy_inputs_for_containers(policy,service_name,inputs)
    for x in list(inputs.keys()):
      log.info('(I)   => "{0}": {1}'.format(x,inputs[x]))
    log.info('(Q) Evaluating queries and alerts for service "{0}" starts'.format(service_name))
    if results:
      queries, alerts = add_query_results_and_alerts_to_service(policy, results, service_name)
    else:
      queries, alerts = prom.evaluate_data_queries_and_alerts_for_a_service(
                             config['prometheus_endpoint'],policy,service_name)
    for attrname, attrvalue in queries.items():
      log.info('(Q)   => "{0}" is "{1}".'.format(attrname,attrvalue))
    for attrname, attrvalue in alerts.items():
      log.info('(A)   => "{0}" is "{1}".'.format(attrname,attrvalue))
    log.info('(P) Policy evaluation for service "{0}" starts'.format(service_name))
    perform_policy_evaluation_on_a_k8s_deploy(policy,service_name)
    log.info('(S) Scaling of service "{0}" starts'.format(service_name))
    perform_service_scaling(policy,service_name)
    for attrname, attrvalue in alerts.items():
      prom.alerts_remove(attrname)

  log.info('--- session finished ---')
  return

def start(policy_yaml):
  global log
  log = logging.getLogger('pk')
  evaluator.init_queue_reading()
  policy = prepare_session(policy_yaml)
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
  evaluator.stop_queue_reading()

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
    print('ERROR: Cannot read configuration file "{0}": {1}'.format(args.cfg_path,str(e)))
  config = pk_config.config()
  #initialise logging facility based on the configuration
  try:
    logging.config.dictConfig(config['logging'])
    log = logging.getLogger('pk')
  except Exception as e:
    print('ERROR: Cannot process configuration file "{0}": {1}'.format(args.cfg_path,str(e)))
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
    pk_rest.init_logging()
    evaluator.init_logging()
    pk_rest.app.run(debug=True,
                    host=args.host,
                    port=args.port)

if __name__ == '__main__':
  pkmain()
