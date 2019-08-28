import logging
import requests
from ruamel import yaml
import shutil,os
import pk_config
from pk_helper import *
import json,time

DEFAULT_prestr_init = 'm_opt_init_'
DEFAULT_prestr_input = 'm_opt_input_'
DEFAULT_prestr_target = 'm_opt_target_'
DEFAULT_prestr_target_query = 'query_'
DEFAULT_prestr_target_minth = 'minth_'
DEFAULT_prestr_target_maxth = 'maxth_'

m_opt_init_params = dict()
m_opt_variables = list()
m_opt_dummy_advice = dict(valid='False',phase='training',vmnumber=0,errmsg='Optimizer is disabled! (dryrun)',confident=0)
m_opt_accessible = True
dryrun_id = 'optimizer'

"""
Description of the DATA STRUCTURES
----------------------------------

Format of optimizer interface for init, input and target parameters
as they must be specified in the policy description:

data:
  constants:
    m_opt_init_VAR1: 'value' 
    ...
  queries:
    m_opt_input_VAR2: 'query expr for VAR2' ==> params specified this way are sent as sample for the optimiser
    m_opt_input_VAR3: 'query expr for VAR3'
    ...
    m_opt_target_query_TARGET1: 'query expre for TARGET1' ==> params specified this way are sent 
    m_opt_target_minth_TARGET1: 5
    m_opt_target_maxth_TARGET1: 10
    ...

m_opt_init_...  : these params are sent as initialisation parameter for the optimizer
m_opt_input_... : these params are evaluated and sent as sample in each cycle for the optimizer
m_opt_target_...: these params are are used both in init and sample communication
             query_...: to specify the query expression to be evaluated and sent as sample in each cycle
             minth_...: to specify the minimum threshold for the target variable to be sent as init param
             maxth_...: to specify the maximum threshold for the target variable to be sent as init param

=====================================

The following dict (converted later to YAML) is required 
when calling the Optimizer REST API initialization method.
This data structure is built based on the above policy description.

m_opt_init_params = {
'varname1': 'value1'
'varname2': 'value2'
...
input_metrics: [
  { name: 'varname3' }
  { name: 'varname4' } 
  ... ]
target_metrics: [
  { name: 'varname3', min_threshold: 'value5', max_threshold: 'value6' }
  { name: 'varname4', min_threshold: 'value7', max_threshold: 'value8' }
  ... ] }

=====================================

The following list is required to identify the evaluated variables 
that are needed when calling the Optimizer REST API sample method.
This data structure is built based on the above policy description.

m_opt_variables = 
[ { lname: 'name of variable in its original form: "m_opt_{input/target_query}_VARNAME"
    sname: 'name of variable used towards the Optimizer'
    query: 'query string associated to the variable'
  }
  ...
]

=====================================

"""

def reset_variables():
  m_opt_init_params.update(dict())
  m_opt_variables[:] = []
  return

def varname_if_init(varname):
  config = pk_config.config()
  init_prestr=config.get('optimizer_vars_prefix_init',DEFAULT_prestr_init)
  if varname.startswith(init_prestr):
    return varname[len(init_prestr):]
  else:
    return None

def varname_if_input(varname):
  config = pk_config.config()
  input_prestr=config.get('optimizer_vars_prefix_input',DEFAULT_prestr_input)
  if varname.startswith(input_prestr):
    return varname[len(input_prestr):]
  else:
    return None

def check_if_target(varname):
  config = pk_config.config()
  prestr_target=config.get('optimizer_vars_prefix_target',DEFAULT_prestr_target)
  return varname.startswith(prestr_target)

def insert_target_structure(m_opt_init_params,key,value):
  log=logging.getLogger('pk_optimizer')
  config = pk_config.config()
  prestr_target = config.get('optimizer_vars_prefix_target',DEFAULT_prestr_target)
  prestr_target_query = prestr_target+config.get('optimizer_vars_prefix_target_query',DEFAULT_prestr_target_query)
  varname, fieldname = None, None
  if key.startswith(prestr_target_query):
    varname=key[len(prestr_target_query):]
    fieldname='name'
    m_opt_variables.append(dict(lname=key,sname=varname,query=value))
  prestr_target_minth = prestr_target+config.get('optimizer_vars_prefix_target_minth',DEFAULT_prestr_target_minth)
  if key.startswith(prestr_target_minth):
    varname=key[len(prestr_target_minth):]
    fieldname='min_threshold'
  prestr_target_maxth = prestr_target+config.get('optimizer_vars_prefix_target_maxth',DEFAULT_prestr_target_maxth)
  if key.startswith(prestr_target_maxth):
    varname=key[len(prestr_target_maxth):]
    fieldname='max_threshold'
  if varname and fieldname:
    log.info('(O)   => TARGET: {0}/{1}:{2}'.format(varname,fieldname,value))
    for atarget in m_opt_init_params['constants']['target_metrics']:
      if atarget['name']==varname:
        if fieldname!='name':
          atarget[fieldname]=value
        return
    targetdict = dict()
    targetdict[fieldname] = value
    targetdict['name'] = varname
    m_opt_init_params['constants']['target_metrics'].append(targetdict)
  return    

def collect_init_params_and_variables(policy):
  log=logging.getLogger('pk_optimizer')
  config = pk_config.config()
  if pk_config.dryrun_get(dryrun_id):
    log.info('(O)   DRYRUN enabled. Skipping...')
    return
  reset_variables()
  m_opt_init_params['constants'] = dict()
  for varname,value in policy.get('data',dict()).get('constants',dict()).iteritems():
    retvarname = varname_if_init(varname)
    if retvarname:
      log.info('(O)   => INIT: {0}:{1}'.format(retvarname,value))
      m_opt_init_params['constants'][retvarname]=value
  m_opt_init_params['constants']['input_metrics']=list()
  for varname,query in policy.get('data',dict()).get('queries',dict()).iteritems():
    retvarname = varname_if_input(varname)
    if retvarname:
      log.info('(O)   => INPUT: {0}:{1}'.format(retvarname,query))
      m_opt_init_params['constants']['input_metrics'].append(dict(name=retvarname))
      m_opt_variables.append(dict(lname=varname,sname=retvarname,query=query))
  m_opt_init_params['constants']['target_metrics']=list()
  for varname,query in policy.get('data',dict()).get('queries',dict()).iteritems():
    if check_if_target(varname):
      insert_target_structure(m_opt_init_params,varname,query)
  for onenode in policy.get('scaling',dict()).get('nodes',[]):
    if 'm_opt_advice' in onenode.get('scaling_rule',''):
      _,omin,omax = limit_instances(None,
                                    onenode.get('min_instances'),
                                    onenode.get('max_instances'))
      m_opt_init_params['constants']['min_vm_number']=omin
      m_opt_init_params['constants']['max_vm_number']=omax
  log.debug('(O) m_opt_init_params (yaml) => {0}'.format(yaml.dump(m_opt_init_params)))
  log.debug('(O) m_opt_variables (yaml) => {0}'.format(yaml.dump(m_opt_variables)))
  return

def calling_rest_api_init():
  global m_opt_accessible
  log=logging.getLogger('pk_optimizer')
  config = pk_config.config()
  if pk_config.dryrun_get(dryrun_id):
    log.info('(O)   DRYRUN enabled. Skipping...')
    return
  url = config.get('optimizer_endpoint')+'/optimizer/init'
  log.debug('(O) Calling optimizer REST API init() method: '+url)
  try:
    response = requests.post(url, data=yaml.dump(m_opt_init_params))
    m_opt_accessible = True
  except Exception as e:
    m_opt_accessible = False
    log.exception('(O) Calling optimizer REST API init() method raised exception: ')
    log.info('(O) WARNING: Optimizer is disabled for the current policy.')
    return
  log.debug('(O) Response: '+str(response))
  return

def generate_sample(userqueries=dict(),sysqueries=dict()):
  log=logging.getLogger('pk_optimizer')
#  if pk_config.dryrun_get(dryrun_id):
#    log.info('(O)   DRYRUN enabled. Skipping...')
#    return dict()
#  if not m_opt_accessible:
#    return dict()
  log.debug('(O)  USRQUERIES: {0}'.format(str(userqueries)))
  log.debug('(O)  SYSQUERIES: {0}'.format(str(sysqueries)))
  sample = dict()
  sample['sample']=dict()
  sample['sample']['input_metrics']=[]
  sample['sample']['target_metrics']=[]

  for var in m_opt_variables:
    log.debug('(O)  => Scanning {0} ...'.format(var['lname']))
    onesample=dict()
    onesample['name']=var['sname']
    onesample['value']=None
    for vname,vvalue in userqueries.iteritems():
      if vname==var['lname']:
        onesample['value']=vvalue
    if onesample['value'] is not None:
      if check_if_target(var['lname']):
        sample['sample']['target_metrics'].append(onesample)
      else:
        sample['sample']['input_metrics'].append(onesample)
  sample['sample']['timestamp']=str(time.time()).split('.')[0]
  sample['sample']['vm_number']=max(len(sysqueries.get('m_nodes',[])),1)
  log.debug('(O)  => Generated sample: '+str(sample))
  return sample

def calling_rest_api_sample(sample=dict()):
  log=logging.getLogger('pk_optimizer')
  config = pk_config.config()
  if pk_config.dryrun_get(dryrun_id):
    log.info('(O)   DRYRUN enabled. Skipping...')
    return 
  if not m_opt_accessible:
    return
  url = config.get('optimizer_endpoint')+'/optimizer/sample'
  log.debug('(O) Calling optimizer REST API sample() method: '+url)
  response = requests.post(url, data=yaml.dump(sample))
  log.debug('(O) Response: '+str(response))
  return

def calling_rest_api_advice():
  log=logging.getLogger('pk_optimizer')
  if pk_config.dryrun_get(dryrun_id) or not m_opt_accessible:
    return m_opt_dummy_advice
  config = pk_config.config()
  url = config.get('optimizer_endpoint')+'/optimizer/advice'
  log.debug('(O) Calling optimizer REST API advice() method: '+url)
  response = requests.get(url).json()
  log.debug('(O) Response: {0}'.format(response))
  return response
