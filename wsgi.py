# -*- coding: utf-8 -*-

import os
import sys
import re
from threading import Thread, Event
import logging
from flask_cache_bust import init_cache_busting
from pogom import config
from pogom.utils import get_args, now
from pogom.app import Pogom
from pogom.models import init_database, create_tables


# Patch to make exceptions in threads cause an exception.
def install_thread_excepthook():
    """
    Workaround for sys.excepthook thread bug
    (https://sourceforge.net/tracker/?func=detail&atid=105470&aid=1230540&group_id=5470).
    Call once from __main__ before creating any threads.
    If using psyco, call psycho.cannotcompile(threading.Thread.run)
    since this replaces a new-style class method.
    """
    import sys
    run_old = Thread.run

    def run(*args, **kwargs):
        try:
            run_old(*args, **kwargs)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            sys.excepthook(*sys.exc_info())
    Thread.run = run


# Exception handler will log unhandled exceptions.
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    log.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


# Patch threading to make exceptions catchable.
install_thread_excepthook()

# Make sure exceptions get logged.
sys.excepthook = handle_exception

args = get_args()

logging.basicConfig(format='%(asctime)s [%(threadName)16s][%(module)14s][%(levelname)8s] %(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

# Let's not forget to run Grunt.
if not os.path.exists(os.path.join(os.path.dirname(__file__), 'static/dist')):
    log.critical('Missing front-end assets (static/dist) -- please run "npm install && npm run build" before starting the server.')
    sys.exit()

# These are very noisy, let's shush them up a bit.
logging.getLogger('peewee').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('pgoapi.pgoapi').setLevel(logging.WARNING)
logging.getLogger('pgoapi.rpc_api').setLevel(logging.INFO)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

prog = re.compile("^(\-?\d+\.\d+),?\s?(\-?\d+\.\d+)$")
res = prog.match(args.location)
position = (float(res.group(1)), float(res.group(2)), 0)

config['LOCALE'] = args.locale

application = Pogom(__name__)
db = init_database(application)

application.set_current_location(position)

# Control the search status (running or not) across threads.
pause_bit = Event()
pause_bit.clear()
if args.on_demand_timeout > 0:
    pause_bit.set()

heartbeat = [now()]

# No more stale JS.
init_cache_busting(application)

application.set_search_control(pause_bit)
application.set_heartbeat_control(heartbeat)

config['ROOT_PATH'] = application.root_path
config['GMAPS_KEY'] = args.gmaps_key


if __name__ == '__main__':
    application.run(threaded=True, use_reloader=False, debug=False)
