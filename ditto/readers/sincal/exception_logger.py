'''
Simple decorator to log uncaught exceptions in a decorated function.
Useful when calling things inside threads that suppress exceptions you want logged without writing lots of boilerplate exception handling code.
This also makes sure that exceptions end up in a log file (if so configured), rather than just being printed to stderr.
@author: Sam.West@csiro.au
'''
import logging
def log_exceptions(func):
    logger = logging.getLogger(__name__)
    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            logger.error(f'Unhandled exception in {func.__name__}', exc_info=True)
    return inner_function
