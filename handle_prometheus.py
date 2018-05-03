import logging
import requests
from ruamel import yaml
import handle_docker as dock
import shutil,os
import pk_config

alerts = {}

def is_subdict(subdict=dict(),maindict=dict()):
  return all((k in maindict and maindict[k]==v) for k,v in subdict.iteritems())

def extract_value_from_prometheus_response(expression,response,filterdict=dict()):
  log=logging.getLogger('pk_prometheus')
  if response.get('status') != 'success' or \
    response.get('data',dict()).get('result',None) is None or \
    not isinstance(response['data']['result'],list):
      raise Exception('Unrecognised prometheus response for expression "{0}": "{1}"'
                      .format(expression,str(response)))
  if response['data']['resultType']=='vector':
    result = [ x for x in response['data']['result'] 
             if x.get('metric',None) is not None and is_subdict(filterdict,x['metric']) ]
    if len(result)>1:
      raise Exception('Multiple results in prometheus response for expression "{0}": "{1}"'
                      .format(expression,str(result)))
    if len(result)<1:
      raise Exception('No results found in prometheus response for expression "{0}": "{1}"'
                      .format(expression,str(result)))
    if not result[0].get('value'):
      raise Exception('Unrecognised result in prometheus response for expression "{0}": "{1}"'
                      .format(expression,str(result[0])))
    value=result[0]['value']
  else:
    value=response['data']['result']
  if not isinstance(value,list) or \
    not isinstance(value[0],float) or \
    not isinstance(value[1],basestring):
      raise Exception('Unrecognised value in prometheus response for expression "{0}": "{1}"'
                      .format(expression,str(value)))
  return value[1]

def filter_data_queries_by_target(queries,target):
  result=dict()
  for param,query in queries.iteritems():
    if target.find(param)!= -1:
      result[param]=query

def evaluate_data_queries_for_nodes(endpoint,policy):
  log=logging.getLogger('pk_prometheus')
  items = dict()
  if 'query_results' not in policy['data']:
    policy['data']['query_results']=dict()
  if 'queries' in policy['data']:
    target_str = policy.get('scaling',dict()).get('nodes',dict()).get('target','')
    for param,query in policy['data']['queries'].iteritems():
      try:
        if target_str is not None and target_str.find(param) != -1:
          if pk_config.simulate():
            continue
          response = requests.get(endpoint+"/api/v1/query?query="+query).json()
          log.debug('Prometheus response query "{0}":{1}'.format(query,response))
          val = extract_value_from_prometheus_response(query,response,dict())
          policy['data']['query_results'][param]=float(val)
          items[param]=float(val)
      except Exception as e:
        policy['data']['query_results'][param]=None
        items[param]=None
        log.warning('Evaluating expression for query "{0}" failed: {1}'.format(param,e.message))
  return items

def evaluate_data_queries_for_a_service(endpoint,policy,servicename):
  log=logging.getLogger('pk_prometheus')
  items = dict()
  if 'query_results' not in policy['data']:
    policy['data']['query_results']=dict()
  if 'queries' in policy['data']:
    all_services = policy.get('scaling',dict()).get('services',dict())
    target_service = [ srv for srv in all_services if srv.get('name','')==servicename ]
    target_str = target_service[0].get('target','') if target_service else ''
    for param,query in policy['data']['queries'].iteritems():
      try:
        if target_str is not None and target_str.find(param) != -1:
          if pk_config.simulate():
            continue
          response = requests.get(endpoint+"/api/v1/query?query="+query).json()
          log.debug('Prometheus response query "{0}":{1}'.format(query,response))
          val = extract_value_from_prometheus_response(query,response,dict())
          policy['data']['query_results'][param]=float(val)
          items[param]=float(val)
      except Exception as e:
        policy['data']['query_results'][param]=None
        items[param]=None
        log.warning('Evaluating expression for query "{0}" failed: {1}'.format(param,e.message))
  return items

def add_exporters_to_prometheus_config(policy, template_file, config_file):
  log=logging.getLogger('pk_prometheus')
  try:
    config_content = dict()
    if not pk_config.simulate():
      with open(template_file,'r') as f:
        config_content = yaml.round_trip_load(f)
    if 'scrape_configs' not in config_content:
      config_content['scrape_configs']=[]
    #Find proper scrape_config or create
    scrape_config = [ x for x in config_content['scrape_configs'] 
		      if x.get('job_name','')=='micado' and 'static_configs' in x ]
    if not scrape_config:
      config_content['scrape_configs'].append({'job_name': 'micado','static_configs':[]})
      scrape_config = [ x for x in config_content['scrape_configs']
		      if x.get('job_name','')=='micado' and 'static_configs' in x ][0]
    else:
      scrape_config = scrape_config[0]
    #Find proper static_config or create
    static_config = [ x for x in scrape_config['static_configs']
		    if 'targets' in x.keys() ]
    if not static_config:
      scrape_config['static_configs'].append({'targets': []})
      static_config = [ x for x in scrape_config['static_configs']
		      if 'targets' in x.keys() ][0]
    else:
      static_config = static_config[0]

    config_changed = False 
    for exporter_endpoint in policy.get('data',dict()).get('sources',dict()):
      if exporter_endpoint not in static_config['targets']:
        static_config['targets'].append(exporter_endpoint)
        config_changed = True
        log.info('(C) => exporter "{0}" added to config'.format(exporter_endpoint))
      else:
        log.info('(C) => exporter "{0}" skipped, already part of config'.format(exporter_endpoint))

    if config_changed and not pk_config.simulate():
      with open(config_file, 'w') as outfile:
        yaml.round_trip_dump(config_content, outfile, default_flow_style=False)

  except Exception as e:
    log.exception('Adding exporters to prometheus config failed:')

  return

def remove_exporters_from_prometheus_config(template_file, config_file):
  if not pk_config.simulate():
    shutil.copyfile(template_file, config_file)

def attach_prometheus_to_exporters_network(policy,swarm_endpoint):
  log=logging.getLogger('pk_prometheus')
  for exporter_endpoint in policy.get('data',dict()).get('sources',dict()):
    try:
      exporter_name=exporter_endpoint.split(':')[0]
      if '.' not in exporter_name:
        log.info('(C) => attaching prometheus to network of exporter "{0}"'.format(exporter_endpoint))
        if pk_config.simulate():
          continue
        exporter_netid = dock.query_service_network(swarm_endpoint,policy['stack'],exporter_name)
        if exporter_netid:
          dock.attach_container_to_network(swarm_endpoint, 'prometheus', exporter_netid)
    except Exception as e:
      log.exception('Attaching prometheus to network of exporter failed:')

def detach_prometheus_from_exporters_network(policy,swarm_endpoint):
  log=logging.getLogger('pk_prometheus')
  for exporter_endpoint in policy.get('data',dict()).get('sources',dict()):
    try:
      exporter_name=exporter_endpoint.split(':')[0]
      if '.' not in exporter_name:
        log.info('(C) => detaching prometheus from network of exporter "{0}"'.format(exporter_endpoint))
        if pk_config.simulate():
          continue
        exporter_netid = dock.query_service_network(swarm_endpoint,policy['stack'],exporter_name)
        if exporter_netid:
          dock.detach_container_from_network(swarm_endpoint, 'prometheus', exporter_netid)
    except Exception as e:
      log.exception('Detaching prometheus from network of exporter failed:')

def notify_to_reload_config(endpoint):
  log=logging.getLogger('pk_prometheus')
  try:
    if not pk_config.simulate():
      requests.post(endpoint+"/-/reload")
    log.info('(C) Notification to reload config sent to Prometheus.')
  except Exception:
    log.exception('Sending config reload notification to Prometheus failed:')

'''
''   Prometheus ALERTING
'''

def deploy_alerts_under_prometheus(rules_directory,alerts,stack):
  if not alerts:
    return
  log=logging.getLogger('pk_prometheus')
  try:
    content={'groups': [ { 'name': 'micado', 'rules' : [] } ] }
    for alert in alerts:
      content['groups'][0]['rules'].append(dict(alert))
    rule_file=os.path.join(rules_directory,stack+'.rules')
    if not pk_config.simulate():
      with open(rule_file, 'w') as outfile:
        yaml.round_trip_dump(content, outfile, default_flow_style=False)
  except Exception:
    log.exception('Deploying alerts under Prometheus failed:')
  return

def remove_alerts_under_prometheus(rules_directory,alerts,stack):
  if not alerts:
    return
  log=logging.getLogger('pk_prometheus')
  try:
    rule_file=os.path.join(rules_directory,stack+'.rules')
    if not pk_config.simulate():
      os.remove(rule_file)
  except Exception:
    log.exception('Removing alerts under Prometheus failed:')
  return

def alerts_isany():
  global alerts
  return True if alerts else False

def alerts_remove(name):
  global alerts
  alerts.pop(name,None) if name else alerts.clear()

def alerts_add(alert):
  global alerts
  stored_alerts = []
  for a in alert.get('alerts'):
    name = a.get('labels',dict()).get('alertname')
    if a.get('status') != 'firing':
      continue
    if name in alerts:
      log=logging.getLogger('pk_prometheus')
      log.warning('(A) Alert "{0}" is already among unhandled alerts!'.format(name))
    alerts[name] = a.get('endsAt')
    stored_alerts.append(name)
  return stored_alerts

def alerts_query(name):
  global alerts
  if not name:
    return alerts
  return alerts[name] if name in alerts else None

