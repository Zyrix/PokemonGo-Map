import re
from threading import Event
import logging
from flask_cache_bust import init_cache_busting
from pogom import config
from pogom.utils import get_args, now
from pogom.app import Pogom
from pogom.models import init_database, create_tables

logging.basicConfig(format='%(asctime)s [%(threadName)16s][%(module)14s][%(levelname)8s] %(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

args = get_args()
config['LOCALE'] = args.locale
config['GMAPS_KEY'] = args.gmaps_key

application = Pogom(__name__)
db = init_database(application)
create_tables(db)

# No more stale JS.
init_cache_busting(application)

pause_bit = Event()
pause_bit.clear()
application.set_search_control(pause_bit)
application.set_heartbeat_control([int(now())])

prog = re.compile("^(\-?\d+\.\d+),?\s?(\-?\d+\.\d+)$")
res = prog.match(args.location)
position = (float(res.group(1)), float(res.group(2)), 0)
application.set_current_location(position)

print position

if __name__ == '__main__':
    application.run()
