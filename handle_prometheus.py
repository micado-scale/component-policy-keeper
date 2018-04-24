import logging
import requests
from ruamel import yaml
import handle_docker as dock

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

def add_exporters_to_prometheus_config(policy, template, config_file):
  log=logging.getLogger('pk_prometheus')
  try:
    with open(template,'r') as f:
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

    if config_changed:
      with open(config_file, 'w') as outfile:
        yaml.round_trip_dump(config_content, outfile, default_flow_style=False)

  except Exception as e:
    log.exception('Adding exporters to prometheus config failed:')

  return

def attach_prometheus_exporters_network(policy,swarm_endpoint):
  log=logging.getLogger('pk_prometheus')
  for exporter_endpoint in policy.get('data',dict()).get('sources',dict()):
    try:
      exporter_name=exporter_endpoint.split(':')[0]
      if '.' not in exporter_name:
        log.info('(C) => adding network of exporter "{0}" to prometheus'.format(exporter_endpoint))
        exporter_netid = dock.query_service_network(swarm_endpoint,policy['stack'],exporter_name)
        if exporter_netid:
          dock.attach_container_to_network(swarm_endpoint, 'prometheus', exporter_netid)
    except Exception as e:
      log.exception('Attaching prometheus to network of exporter failed:')

def notify_to_reload_config(endpoint):
  log=logging.getLogger('pk_prometheus')
  try:
    requests.post(endpoint+"/-/reload")
    log.info('(C) Notification to reload config sent to Prometheus.')
  except Exception as e:
    log.exception('Sending config reload notification to Prometheus failed:')

  
