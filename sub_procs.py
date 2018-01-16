from subprocess import Popen, PIPE
from threading import Thread

procs = {}  # hashmap: uuid - process

def add_task(uuid, pid, proc_ref):
    procs[str(uuid)] = {'pid': pid, 'process': proc_ref}

def remove_task(uuid):
    del procs[str(uuid)]

def get_task(uuid):
    try:
        procs[str(uuid)]
    except KeyError:
        return None
    else:
        return procs[str(uuid)]

def create_subprocess(uuid, obj_type, callback):
    t = Thread(target=handle_process, args=(uuid, obj_type, callback))
    t.start()
    tid = t.ident
    return (True, tid)

def kill_process(uuid):
    process_def = get_task(uuid)
    if process_def:
        process = process_def['process']
        process.kill()
        return True
    else:
        return False
    
def handle_process(uuid, obj_type, callback):
        process = Popen(['python3', obj_type, uuid], stdout=PIPE, stderr=PIPE)
        pid = process.pid
        add_task(uuid, pid, process)
        stdout, stderr = process.communicate()
        return_code = process.returncode
        if stderr.decode('utf-8') == '' and return_code >= 0:
            res = 'SUCCESS'
        else:
            res = 'FAIL'
        controller, callback_fn = callback
        remove_task(uuid)
        callback_fn(uuid, res)

