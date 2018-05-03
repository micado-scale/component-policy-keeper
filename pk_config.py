
var_config = None
var_policy = None
var_finish_scaling = False
var_simulate = False

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
