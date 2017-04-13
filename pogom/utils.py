#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import configargparse
import os
import math
import json
import logging
import shutil
import pprint
import time
from s2sphere import CellId, LatLng

from . import config

log = logging.getLogger(__name__)


def parse_unicode(bytestring):
    decoded_string = bytestring.decode(sys.getfilesystemencoding())
    return decoded_string


def verify_config_file_exists(filename):
    fullpath = os.path.join(os.path.dirname(__file__), filename)
    if not os.path.exists(fullpath):
        log.info('Could not find %s, copying default', filename)
        shutil.copy2(fullpath + '.example', fullpath)


def memoize(function):
    memo = {}

    def wrapper(*args):
        if args in memo:
            return memo[args]
        else:
            rv = function(*args)
            memo[args] = rv
            return rv
    return wrapper


@memoize
def get_args():
    # Pre-check to see if the -cf or --config flag is used on the command line.
    # If not, we'll use the env var or default value. This prevents layering of
    # config files, and handles missing config.ini as well.
    defaultconfigfiles = [os.getenv('POGOMAP_CONFIG', os.path.join(os.path.dirname(__file__), '../config/config.ini'))]
    parser = configargparse.ConfigFileParser()
    with open('config/config.ini') as file:
        args = parser.parse(file)

    args = dict(args)

    class Struct:
        def __init__(self, **entries):
            self.__dict__.update(entries)

    args = Struct(**args)

    if args.location is None:
        parser.print_usage()
        print(sys.argv[0] + ": error: arguments -l/--location is required.")
        sys.exit(1)

    return args


def now():
    # The fact that you need this helper...
    return int(time.time())


# gets the time past the hour
def cur_sec():
    return (60 * time.gmtime().tm_min) + time.gmtime().tm_sec


# gets the total seconds past the hour for a given date
def date_secs(d):
    return d.minute * 60 + d.second


# checks to see if test is between start and end assuming roll over like a clock
def clock_between(start, test, end):
    return (start <= test <= end and start < end) or (not (end <= test <= start) and start > end)


# return amount of seconds between two times on the clock
def secs_between(time1, time2):
    return min((time1 - time2) % 3600, (time2 - time1) % 3600)


# Return the s2sphere cellid token from a location
def cellid(loc):
    return CellId.from_lat_lng(LatLng.from_degrees(loc[0], loc[1])).to_token()


# Return equirectangular approximation distance in km
def equi_rect_distance(loc1, loc2):
    R = 6371  # radius of the earth in km
    lat1 = math.radians(loc1[0])
    lat2 = math.radians(loc2[0])
    x = (math.radians(loc2[1]) - math.radians(loc1[1])) * math.cos(0.5 * (lat2 + lat1))
    y = lat2 - lat1
    return R * math.sqrt(x * x + y * y)


# Return True if distance between two locs is less than distance in km
def in_radius(loc1, loc2, distance):
    return equi_rect_distance(loc1, loc2) < distance


def i8ln(word):
    if config['LOCALE'] == "en":
        return word
    if not hasattr(i8ln, 'dictionary'):
        file_path = os.path.join(
            config['ROOT_PATH'],
            config['LOCALES_DIR'],
            '{}.min.json'.format(config['LOCALE']))
        if os.path.isfile(file_path):
            with open(file_path, 'r') as f:
                i8ln.dictionary = json.loads(f.read())
        else:
            log.warning('Skipping translations - Unable to find locale file: %s', file_path)
            return word
    if word in i8ln.dictionary:
        return i8ln.dictionary[word]
    else:
        log.debug('Unable to find translation for "%s" in locale %s!', word, config['LOCALE'])
        return word


def get_pokemon_data(pokemon_id):
    if not hasattr(get_pokemon_data, 'pokemon'):
        file_path = os.path.join(
            config['ROOT_PATH'],
            config['DATA_DIR'],
            'pokemon.min.json')

        with open(file_path, 'r') as f:
            get_pokemon_data.pokemon = json.loads(f.read())
    return get_pokemon_data.pokemon[str(pokemon_id)]


def get_pokemon_name(pokemon_id):
    return i8ln(get_pokemon_data(pokemon_id)['name'])


def get_pokemon_rarity(pokemon_id):
    return i8ln(get_pokemon_data(pokemon_id)['rarity'])


def get_pokemon_types(pokemon_id):
    pokemon_types = get_pokemon_data(pokemon_id)['types']
    return map(lambda x: {"type": i8ln(x['type']), "color": x['color']}, pokemon_types)


def get_moves_data(move_id):
    if not hasattr(get_moves_data, 'moves'):
        file_path = os.path.join(
            config['ROOT_PATH'],
            config['DATA_DIR'],
            'moves.min.json')

        with open(file_path, 'r') as f:
            get_moves_data.moves = json.loads(f.read())
    return get_moves_data.moves[str(move_id)]


def get_move_name(move_id):
    return i8ln(get_moves_data(move_id)['name'])


def get_move_damage(move_id):
    return i8ln(get_moves_data(move_id)['damage'])


def get_move_energy(move_id):
    return i8ln(get_moves_data(move_id)['energy'])


def get_move_type(move_id):
    move_type = get_moves_data(move_id)['type']
    return {"type": i8ln(move_type), "type_en": move_type}


class Timer():

    def __init__(self, name):
        self.times = [(name, time.time(), 0)]

    def add(self, step):
        t = time.time()
        self.times.append((step, t, round((t - self.times[-1][1]) * 1000)))

    def checkpoint(self, step):
        t = time.time()
        self.times.append(('total @ ' + step, t, t - self.times[0][1]))

    def output(self):
        self.checkpoint('end')
        pprint.pprint(self.times)
