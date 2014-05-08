import logging
import psutil
from tcpy import TCPServer, TCPHandler

try:
    from memwatchconfig import PROFILER_HOST
except:
    from defaultconfig import PROFILER_HOST
try:
    from memwatchconfig import PROFILER_PORT
except:
    from defaultconfig import PROFILER_PORT


logging.basicConfig()
logger = logging.getLogger("memwatch.profiler")


def get_memory_usage(proc):
    """ Record the current memory usage of the given proc. """
    try:
        process = psutil.Process(proc)
        return process.get_memory_info()[0]
    except:
        msg = "Profiler failed to get memory info for process %d!" % proc
        logger.error(msg)


class MemoryProfiler(TCPHandler):

    """
    Implements the Server that will watch
    the block of code we want to profile.

    """

    def __init__(self, opt=None, pid=None, **kwargs):
        self.opt = opt
        self.proc_to_watch = pid
        self.start_mem = get_memory_usage(self.proc_to_watch)
        super(MemoryProfiler, self).__init__(**kwargs)

    def execute(self):
        try:
            # Notify the process that we are ready to begin.
            self.send({"ready": True})

            # Get memory usage until the parent tells us to stop or we timeout.
            max_mem = self.start_mem
            self.conn.sock.settimeout(1.0 / 10000)
            while True:
                curr_mem = get_memory_usage(self.proc_to_watch)
                max_mem = max(curr_mem, max_mem)

                # Poll for stop signal
                try:
                    self.recv()
                    break
                except:
                    continue

            # Send the results back to the parent.
            return self.success(peak_usage=max_mem - self.start_mem)
        except Exception as e:
            msg = e.message
            if not msg:
                msg = "Memory Profiler process died watching %d!" % self.proc_to_watch
            logger.error(msg)
            return self.error(msg)


if __name__ == "__main__":
    server = TCPServer(PROFILER_HOST, PROFILER_PORT)
    server.commands = {
        'profile': MemoryProfiler
    }
    server.listen()
