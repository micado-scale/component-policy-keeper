import logging
from flask import Flask, request, jsonify
import threading
import policy_keeper
from ruamel import yaml
import pk_config

policy_thread = None

app = Flask(__name__)

log = None

def init_service():
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

@app.route('/policy/start', methods=['POST'])
def start_policy():
  global policy_thread
  policy_yaml = request.stream.read()
  if not policy_yaml:
    raise RequestException(400, 'Empty POST data')
  if policy_thread:
    raise RequestException(400, 'Policy processing is already running')
  else:
    log.info('Received policy: {0}'.format(policy_yaml))
    policy_thread = threading.Thread(target=policy_keeper.perform_policy_keeping,args=(policy_yaml,))
    policy_thread.start() 
  return jsonify(dict(response='no error'))

@app.route('/policy/stop', methods=['POST'])
def stop_policy():
  global policy_thread
  if policy_thread:
    pk_config.set_finish_scaling(True)
    policy_thread.join()
    policy_thread = None
  return jsonify(dict(response='no error'))
      
    

