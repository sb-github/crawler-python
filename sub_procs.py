from subprocess import Popen, PIPE

procs = {}


def create_subprocess(obj_type, uid):
    process = Popen(['python3', obj_type, uid],
                    stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    pid = process.pid
    if stderr is None:
        procs[pid] = process
        return True
    else:
        return False

    # also possible to kill process after timeout expires


def check_termination(pid):
    try:
        procs[pid]
    except KeyError:
        pass
    else:
        return procs[pid].poll()  # returns returncode attribute or None


def kill_procs():
    for pid in procs:
        procs[pid].wait()
        procs[pid].kill()
        del procs[pid]
