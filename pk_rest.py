import logging
from flask import Flask, request, jsonify
import threading
import policy_keeper
from ruamel import yaml
import pk_config
import handle_prometheus as prom

policy_thread = None

app = Flask(__name__)

log = None

def init_logging():
  global log 
  log = logging.getLogger('pk_rest')

class RequestException(Exception):
  def __init__(self, status_code, reason, *args):
      super(RequestException, self).__init__(*args)
      self.status_code, self.reason = status_code, reason
  def to_dict(self):
      return dict(status_code=self.status_code,
                  reason=self.reason,
                  message=str(self))

@app.errorhandler(RequestException)
def handled_exception(error):
  log.error('An exception occured: %r', error)
  return jsonify(error.to_dict())

@app.errorhandler(Exception)
def unhandled_exception(error):
  import traceback as tb
  log.error('An unhandled exception occured: %r\n%s',
              error, tb.format_exc(error))
  response = jsonify(dict(message=error.message))
  response.status_code = 500
  return response

@app.route('/policy/eval', methods=['POST'])
def eval_policy():
  global policy_thread
  data_yaml = request.stream.read()
  if not data_yaml:
    raise RequestException(400, 'Empty POST data')
  if policy_thread:
    raise RequestException(400, 'Policy processing is already running')
  else:
    log.info('Received data: {0}'.format(data_yaml))
    policy_yaml = pk_config.policy() 
    policy = yaml.safe_load(policy_yaml)
    policy_keeper.resolve_queries(policy)
    results = yaml.safe_load(data_yaml)
    policy_keeper.perform_one_session(policy, results)
  return jsonify(dict(response='OK'))

@app.route('/policy/set', methods=['POST'])
def set_policy():
  global policy_thread
  policy_yaml = request.stream.read()
  if not policy_yaml:
    raise RequestException(400, 'Empty POST data')
  if policy_thread:
    raise RequestException(400, 'Policy processing is already running')
  else:
    log.info('Received policy: {0}'.format(policy_yaml))
    pk_config.policy(policy_yaml)
  return jsonify(dict(response='OK'))

@app.route('/policy/start', methods=['POST'])
def start_policy():
  global policy_thread
  policy_yaml = request.stream.read()
  if not policy_yaml: 
    if pk_config.policy():
      policy_yaml = pk_config.policy()
    else:
      raise RequestException(400, 'Empty POST data for /policy/start')
  if policy_thread:
    raise RequestException(400, 'Policy processing is already running')
  else:
    log.info('Received policy: {0}'.format(policy_yaml))
    pk_config.finish_scaling(False)
    policy_thread = threading.Thread(target=policy_keeper.perform_policy_keeping,args=(policy_yaml,))
    policy_thread.start() 
  return jsonify(dict(response='OK'))

@app.route('/policy/stop', methods=['POST'])
def stop_policy():
  global policy_thread
  if policy_thread:
    pk_config.finish_scaling(True)
    policy_thread.join()
    policy_thread = None
  return jsonify(dict(response='OK'))
      
@app.route('/alerts/fire', methods=['POST'])
def alerts_fire():
  alert = yaml.safe_load(request.stream)
  a = prom.alerts_add(alert)
  log.info('(A) Alert(s) fired: {0}'.format(a))
  return ''

@app.route('/alerts/reset', methods=['POST'])
def alerts_init():
  alert = yaml.safe_load(request.stream)
  log.info('(A) Resetting alerts based on external request.')
  prom.alerts_remove(None)
  return jsonify(dict(response='OK'))


