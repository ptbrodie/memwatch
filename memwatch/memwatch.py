from functools import wraps
import os
import resource
from tcpy import TCPClient


def profile(key_name, custom_emit=None):
    """
    Returns a decorator which will time a call to a function
    and emit a metric to statsite with the peak memory usage.

    Example:
        @profile("my_function_key", flush_after=True)
        def should_give_reward(a, b, c):
            ....

    """
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            with ProfiledBlock(key_name, custom_emit):
                return func(*args, **kwargs)
        return wrapped
    return decorator


class ProfiledBlock(object):
    """
    Implements a context manager that will profile the memory
    consumed by a block of code, and emit memory metrics to statsite.

    Metrics:
        peak_usage: high-water mark for memory usage for a profiled block
        unreturned: memory that was not freed after the block was exited (leaks)

    Example:
        with ProfiledBlock("consume_bytes"):
            consume_bytes()

    """

    def __init__(self, block_name, custom_emit=None):
        self.block_name = block_name
        self.pid = os.getpid()
        self.custom_emit = custom_emit if custom_emit else self.emit
        self.profiler = TCPClient(PROFILER_HOST, PROFILER_PORT)
        self.start_mem = None
        self.units = None

    def __enter__(self):
        self.start_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.enable()

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        peak_usage = self.disable()
        end_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

        # Emit the metrics
        unreturned = end_mem - self.start_mem
        try:
            # We try to use a custom emit function
            self.custom_emit(peak_usage, unreturned, self.block_name)
        except:
            print "Custom emit function failed. "
            print "Usage: custom_emit(peak_usage, unreturned, block_name)\n"
            self.emit(peak_usage, end_mem - self.start_mem, self.block_name)

    def enable(self):
        # Send our PID and the start signal to the memwatch server
        self.profiler.execute(cmd="profile", opt="start")

    def disable(self):
        self.profiler.execute(cmd="profile", opt="stop")

    def emit(self, peak_usage, unreturned, block_name):
        if unreturned > 0:
            print ""
            print "POSSIBLE LEAK IN %s" % block_name
            print "Unreturned memory could be an indication of a memory leak."
            print ""
        base_line = "================================"
        line_match = "=" * (len(block_name) + 1)
        line_match += base_line
        print "%s %s" % (block_name, base_line)
        print "Block Memory Usage"
        print "    Peak Usage: %s" % peak_usage
        print "    Unreturned: %s" % unreturned
        print line_match
