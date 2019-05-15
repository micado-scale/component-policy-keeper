
var_config = None
var_policy = None
var_finish_scaling = False
var_simulate = False
var_dryrun = []
var_dryrun_components = ['occopus','k8s','prometheus','optimizer']

def config(nc = None):
  global var_config
  if nc is not None:
    var_config = nc
  return var_config

def finish_scaling(fs = None):
  global var_finish_scaling
  if fs is not None:
    var_finish_scaling = fs
  return var_finish_scaling

def simulate(sim = None):
  global var_simulate
  if sim is not None:
    var_simulate = sim
  return var_simulate

def policy(pol = None):
  global var_policy
  if pol is not None:
    var_policy = pol
  return var_policy

def dryrun_set(component=None,value=False):
  global var_dryrun
  if component is None:
    var_dryrun=var_dryrun_components.copy() if value else list()
  else:
    if component in var_dryrun_components:
      if value:
        if component not in var_dryrun:
          var_dryrun.append(component)
      else:
        if component in var_dryrun:
          var_dryrun.remove(component)
    else:
      raise Exception('ERROR: Invalid component name in dryrun_get() method!')
  return

def dryrun_get(component=None):
  global var_dryrun
  if component is None:
    return var_dryrun
  if component=='' or component not in var_dryrun_components:
    raise Exception('ERROR: Invalid component name in dryrun_get() method!')
  return True if component in var_dryrun else False

  

