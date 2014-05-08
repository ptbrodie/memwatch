from functools import wraps
import logging
import os
import resource
from tcpy import TCPClient

try:
    from memwatchconfig import PROFILER_HOST
except:
    from defaultconfig import PROFILER_HOST
try:
    from memwatchconfig import PROFILER_PORT
except:
    from defaultconfig import PROFILER_PORT

logging.basicConfig()
logger = logging.getLogger("memwatch.memwatch")


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
        self.emit = custom_emit if custom_emit else self.default_emit
        self.profiler = TCPClient(PROFILER_HOST, PROFILER_PORT)
        self.start_mem = None
        self.units = None

    def __enter__(self):
        self.start_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.enable()

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        usage_result = self.disable()
        if not usage_result.get("success", False):
            raise Exception("%s: %s" % (self.block_name, usage_result.get("message")))

        peak_usage = usage_result.get("peak_usage", 0)
        unreturned = usage_result.get("unreturned", 0)

        # Emit the metrics
        try:
            # We try to use a custom emit function
            self.emit(peak_usage, unreturned, self.block_name)
        except:
            logger.error(custom_emit_fail_msg())
            self.default_emit(peak_usage, unreturned, self.block_name)

    def enable(self):
        # Send our PID and the start signal to the memwatch server
        self.profiler.send({"cmd": "profile", "opt": "start", "pid": self.pid})
        self.profiler.recv()

    def disable(self):
        self.profiler.send({"stop": True})
        result = self.profiler.recv()
        self.profiler.conn.finish()
        end_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        unreturned = end_mem - self.start_mem
        result.update({"unreturned": unreturned})
        return result

    def default_emit(self, peak_usage, unreturned, block_name):
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


def custom_emit_fail_msg():
    msg = "Custom emit function failed.\n"
    msg += "Usage/Signature: custom_emit(peak_usage,  # float"
    msg += "                             unreturned,  # float"
    msg += "                             block_name)  # str"
    return msg
