import sys
import time
import multiprocessing
from multiprocessing.queues import Queue
import copy
from asteval import Interpreter, make_symbol_table
import threading
import logging

log = None
queue_store = None
queue_thread = None

def init_logging():
  global log, logstream, queue_store
  log = logging.getLogger('pk_usercode')
 
def init_queue_reading(): 
  global queue_thread, queue_store
  queue_store = StdoutQueue()
  queue_thread = threading.Thread(target=text_catcher,args=(queue_store,))
  queue_thread.start()

def stop_queue_reading():
  global queue_thread, queue_store
  queue_store.close()
  queue_store = None

def text_catcher(queue):
  while True:
    try:
      str=queue.get().rstrip()
    except Exception:
      break
    if str!='':
      log.info(str)

class StdoutQueue(Queue):
    def __init__(self,*args,**kwargs):
        Queue.__init__(self,*args,**kwargs)

    def write(self,msg):
        self.put(msg)

    def flush(self):
        sys.__stdout__.flush()
   
class TimeoutException(Exception):
    """ It took too long to compile and execute. """

class RunnableProcessing(multiprocessing.Process):
    """ Run a function in a child process.

    Pass back any exception received.
    """
    #def __init__(self, func, q, *args, **kwargs):
    def __init__(self, func, *args, **kwargs):
        self.queue = multiprocessing.Queue(maxsize=1)
        #args = (func, q, ) + args
        args = (func, ) + args
        multiprocessing.Process.__init__(self, target=self.run_func,
            args=args, kwargs=kwargs)
        

    #def run_func(self, func, q, *args, **kwargs):
    def run_func(self, func, *args, **kwargs):
        try:
            #sys.stdout = q
            #q.write("\n")
            #result = func(q, *args, **kwargs)
            result = func(*args, **kwargs)
            self.queue.put((True, result))
        except Exception as e:
            self.queue.put((False, e))

    def done(self):
        return self.queue.full()

    def result(self):
        x = self.queue.get()
        #self.queue.close()
        #del self.queue
        return x
        


def timeout(seconds, force_kill=True):
    """ Timeout decorator using Python multiprocessing.

    Courtesy of http://code.activestate.com/recipes/577853-timeout-decorator-with-multiprocessing/
    """
    def wrapper(function):
        def inner(*args, **kwargs):
            queue_store.write('==== [{0}] Executing the user defined algorithm starts... ===='
                              .format(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())))
            now = time.time()
            proc = RunnableProcessing(function, *args, **kwargs)
            proc.start()
            proc.join(seconds)
            if proc.is_alive():
                if force_kill:
                    proc.terminate()
                runtime = time.time() - now
                raise TimeoutException('timed out after {0} seconds'.format(runtime))
            assert proc.done()
            queue_store.write('==== [{0}] Executing the user defined algorithm finished. ===='
                              .format(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())))
            success, result = proc.result()
            if success:
                return result
            else:
                raise result
        return inner
    return wrapper


@timeout(10)
def evaluate(eval_code, input_variables={}, output_variables=[]):
#def evaluate(q, eval_code, input_variables={}, output_variables=[]):
    """Evaluates a given expression, with the timeout given as decorator.

    Args:
        eval_code (str): The code to be evaluated.
        input_variables (dict): dictionary of input variables and their values.
        output_variables (array): array of names of output variables.

    Returns:
        dict: the output variables or empty.

    """
    # FIXME: use_numpy the process blocks infinitely at the return statement
    import time
    sym = make_symbol_table(time=time, use_numpy=True, range=range, **input_variables)
    #print("LOGGER:"+str(log))
    aeval = Interpreter(
        writer = queue_store,
        err_writer = queue_store,
        symtable = sym,
        use_numpy = True,
        no_if = False,
        no_for = False,
        no_while = False,
        no_try = True,
        no_functiondef = True,
        no_ifexp = False,
        no_listcomp = True,
        no_augassign = False, # e.g., a += 1
        no_assert = True,
        no_delete = True,
        no_raise = True,
        no_print = False)


    aeval(eval_code)
    symtable = {x: sym[x] for x in sym if x in output_variables}

    
    return symtable


if __name__ == "__main__":

    # Example 1
    code_example_1 = """
y = 0
for i in range(10000):
    y = y + i
x = 5
"""
    values_1 = evaluate(code_example_1, output_variables=['x', 'y'])
    print("Example 1: Output variables: {}".format(values_1))
    assert values_1['x'] == 5, "x value should be 5"
    assert values_1['y'] == 49995000, "y value should be 49995000"

    # Example 2
    code_example_2 = """
for i in range(101):
    y = y + i
"""
    values_2 = evaluate(code_example_2, input_variables={'y': 10},
        output_variables=['y'])
    print("Example 2: Output variables: {}".format(values_2))
    assert values_2['y'] == 5060, "y value should be 5060"

