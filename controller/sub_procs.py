'''
This module helps to manage thread and subprocess creation.
'''


from subprocess import Popen, PIPE
from threading import Thread

procs = {}  # key - uuid, value - dict with process_id and process reference

def add_task(uuid, pid, proc_ref):
    '''
    Adding task to procs when subprocess is created.
    '''
    procs[str(uuid)] = {'pid': pid, 'process': proc_ref}

def remove_task(uuid):
    '''
    Removing task from procs.
    '''
    task = get_task(uuid)
    del task 

def get_task(uuid):
    '''
    Finds task with give UUID from procs.
    '''
    try:
        procs[str(uuid)]
    except KeyError:
        return None
    else:
        return procs[str(uuid)]

def create_subprocess(uuid, obj_type, callback):
    '''
    Creates thread that invokes subprocess creation.
    ''' 
    t = Thread(target=handle_process, args=(uuid, obj_type, callback))
    t.start()
    tid = t.ident
    if t.is_alive():
        return (True, tid)
    else:
        return (False, None)

def kill_process(uuid):
    '''
    Kill with force subprocess by UUID.
    '''
    process_def = get_task(uuid)
    if process_def:
        process = process_def['process']
        process.kill()
        # remove_task(uuid)
        return True
    else:
        return False
    
def handle_process(uuid, obj_type, callback):
    '''
    Create subprocess to run crawler/parser.
    '''

    process = Popen(['python3', obj_type, uuid], stdout=PIPE, stderr=PIPE)

    callback_fn = callback
    res = 'IN PROCESS'
    callback_fn(uuid, res)
        
    pid = process.pid
    add_task(uuid, pid, process)
    stdout, stderr = process.communicate()
    return_code = process.returncode
    if stderr.decode('utf-8') == '' and return_code >= 0:
        res = 'SUCCESS'
    else:
        res = 'FAIL'  # if there any errors in stderr or terminated with force
    callback_fn(uuid, res)
    remove_task(uuid)

