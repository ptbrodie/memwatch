import logging
import psutil
from tcpy import TCPServer, TCPHandler
import time

try:
    from memwatchconfig import PROFILER_HOST
except:
    from defaultconfig import PROFILER_HOST
try:
    from memwatchconfig import PROFILER_PORT
except:
    from defaultconfig import PROFILER_PORT


logger = logging.getLogger(__name__)


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

    def __init__(self, proc_to_watch):
        self.proc_to_watch = proc_to_watch
        self.start_mem = get_memory_usage(self.proc_to_watch)

    def execute(self):
        try:
            # Notify the process that we are ready to begin.
            ready = True
            self.conn.send(ready)

            # Get memory usage until the parent tells us to stop or we timeout.
            max_mem = self.start_mem
            stop = False
            timeout = time.time() + 10
            while not stop:
                curr_mem = get_memory_usage(self.proc_to_watch)
                max_mem = max(curr_mem, max_mem)

                # Poll for stop signal
                stop = self.conn.recv()
                if time.time() > timeout:
                    raise Exception("Memory Profiler timed out while watching %s!" % self.proc_to_watch)

                # Give the CPU to someone else
                time.sleep(1.0 / 10000)

            # Send the results back to the parent.
            self.success(peak_usage=max_mem - self.start_mem)
            self.conn.finish()
        except Exception as e:
            msg = e.message
            if not msg:
                msg = "Memory Profiler process died watching %d!" % self.proc_to_watch
            # This should be logged
            self.error(msg=msg)
            self.conn.finish()
            logger.error(msg)


if __name__ == "__main__":
    server = TCPServer(PROFILER_HOST, PROFILER_PORT)
    server.commands = {
        'profile': MemoryProfiler
    }
    server.listen()
