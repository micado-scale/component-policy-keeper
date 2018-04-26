
config = None
finish_scaling = False

def set_config(newconfig):
  global config
  config = newconfig

def get_config():
  global config
  return config

def set_finish_scaling(fs):
  global finish_scaling
  finish_scaling = fs

def get_finish_scaling():
  global finish_scaling
  return finish_scaling
