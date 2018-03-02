import logging
import requests

def is_subdict(subdict=dict(),maindict=dict()):
  return all((k in maindict and maindict[k]==v) for k,v in subdict.iteritems())

def extract_value_from_prometheus_response(expression,response,filterdict=dict()):
  log=logging.getLogger('pk_prometheus')
  if response.get('status') != 'success' or \
    response.get('data',dict()).get('result',None) is None or \
    not isinstance(response['data']['result'],list):
      raise Exception('Unrecognised prometheus response for expression \"'+expression+'\": \"'+str(response)+"\"")
  if response['data']['resultType']=='vector':
    result = [ x for x in response['data']['result'] if x.get('metric',None) is not None and is_subdict(filterdict,x['metric']) ]
    if len(result)>1:
      raise Exception('Multiple results in prometheus response for expression \"'+expression+'\": \"'+str(result)+"\"")
    if len(result)<1:
      raise Exception('No results found in prometheus response for expression \"'+expression+'\": \"'+str(result)+"\"")
    if not result[0].get('value'):
      raise Exception('Unrecognised result in prometheus response for expression \"'+expression+'\": \"'+str(result[0])+"\"")
    value=result[0]['value']
  else:
    value=response['data']['result']
  if not isinstance(value,list) or \
    not isinstance(value[0],float) or \
    not isinstance(value[1],basestring):
      raise Exception('Unrecognised value in prometheus response for expression \"'+expression+'\": \"'+str(value)+"\"")
  return value[1]

def evaluate_data_queries(endpoint,policy):
  log=logging.getLogger('pk_prometheus')
  log.debug("--> Start prometheus query session...")
  if 'query_results' not in policy['data']:
    policy['data']['query_results']=dict()
  if 'queries' in policy['data']:
    for param,query in policy['data']['queries'].iteritems():
      try:
        response = requests.get(endpoint+"/api/v1/query?query="+query).json()
        log.debug('Prometheus response query "{0}":{1}'.format(query,response))
        val = extract_value_from_prometheus_response(query,response,dict())
        policy['data']['query_results'][param]=float(val)
      except Exception as e:
        policy['data']['query_results'][param]=None
        log.exception('Policy Keeper')
  log.debug("--> End of prometheus query session.")

