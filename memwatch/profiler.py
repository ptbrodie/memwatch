import psutil
import time


def get_memory_usage(proc):
    """ Record the current memory usage of the given proc. """
    try:
        process = psutil.Process(proc)
        return process.get_memory_info()[0]
    except:
        print "Profiler failed to get memory info for process %d!" % proc


class MemoryProfiler(TCPServer):

    """
    Implements the Profiler Process that is forked to watch
    the block of code that we want to profile.

    """

    def __init__(self, proc_to_watch, pipe):
        self.proc_to_watch = proc_to_watch
        self.pipe = pipe
        self.start_mem = get_memory_usage(self.proc_to_watch)
        super(MemoryProfiler, self).__init__()

    def run(self):
        try:
            # Notify the parent that we are ready to begin.
            ready = True
            self.pipe.send(ready)

            # Get memory usage until the parent tells us to stop or we timeout.
            max_mem = self.start_mem
            stop = False
            timeout = time.time() + 10
            while not stop:
                curr_mem = get_memory_usage(self.proc_to_watch)
                max_mem = max(curr_mem, max_mem)

                # Poll for stop signal
                stop = self.pipe.poll()
                if time.time() > timeout:
                    raise Exception("Memory Profiler timed out while watching %s!" % self.proc_to_watch)

                # Give the CPU to someone else
                time.sleep(1.0 / 10000)

            # Send the results back to the parent.
            self.pipe.send(max_mem - self.start_mem)
            self.pipe.close()
        except Exception as e:
            self.pipe.send(-1)
            self.pipe.close()
            msg = e.message
            if not msg:
                msg = "Memory Profiler process died watching %d!" % self.proc_to_watch
            # This should be logged
            print msg


if __name__ == "__main__":
    server = MemoryProfiler
