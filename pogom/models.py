#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import itertools
import calendar
import sys
import traceback
import gc
import time
import geopy
import math
from peewee import SqliteDatabase, InsertQuery, \
    Check, CompositeKey, ForeignKeyField, \
    IntegerField, CharField, DoubleField, BooleanField, \
    DateTimeField, fn, DeleteQuery, FloatField, SQL, TextField, JOIN
from playhouse.flask_utils import FlaskDB
from playhouse.pool import PooledMySQLDatabase
from playhouse.shortcuts import RetryOperationalError
from playhouse.migrate import migrate, MySQLMigrator, SqliteMigrator
from datetime import datetime, timedelta
from base64 import b64encode
from cachetools import TTLCache
from cachetools import cached

from . import config
from .utils import get_pokemon_name, get_pokemon_rarity, get_pokemon_types, get_args, \
    cellid, in_radius, date_secs, clock_between, secs_between, get_move_name, get_move_damage, \
    get_move_energy, get_move_type
from .transform import transform_from_wgs_to_gcj, get_new_coords
from .customLog import printPokemon
log = logging.getLogger(__name__)

args = get_args()
flaskDb = FlaskDB()
cache = TTLCache(maxsize=100, ttl=60 * 5)

db_schema_version = 10


class MyRetryDB(RetryOperationalError, PooledMySQLDatabase):
    pass


def init_database(app):
    if args.db_type == 'mysql':
        log.info('Connecting to MySQL database on %s:%i', args.db_host, int(args.db_port))
        connections = int(args.db_max_connections)
        if hasattr(args, 'accounts'):
            connections *= len(args.accounts)
        db = MyRetryDB(
            args.db_name,
            user=args.db_user,
            password=args.db_pass,
            host=args.db_host,
            port=int(args.db_port),
            max_connections=connections,
            stale_timeout=300)
    else:
        log.info('Connecting to local SQLite database')
        db = SqliteDatabase(args.db)

    app.config['DATABASE'] = db
    flaskDb.init_app(app)
    return db


class BaseModel(flaskDb.Model):

    @classmethod
    def get_all(cls):
        results = [m for m in cls.select().dicts()]
        if args.china:
            for result in results:
                result['latitude'], result['longitude'] = \
                    transform_from_wgs_to_gcj(
                        result['latitude'], result['longitude'])
        return results


class Pokemon(BaseModel):
    # We are base64 encoding the ids delivered by the api,
    # because they are too big for sqlite to handle.
    encounter_id = CharField(primary_key=True, max_length=50)
    spawnpoint_id = CharField(index=True)
    pokemon_id = IntegerField(index=True)
    latitude = DoubleField()
    longitude = DoubleField()
    disappear_time = DateTimeField(index=True)
    individual_attack = IntegerField(null=True)
    individual_defense = IntegerField(null=True)
    individual_stamina = IntegerField(null=True)
    move_1 = IntegerField(null=True)
    move_2 = IntegerField(null=True)
    last_modified = DateTimeField(null=True, index=True, default=datetime.utcnow)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)

    @staticmethod
    def get_active(swLat, swLng, neLat, neLng, timestamp=0, oSwLat=None, oSwLng=None, oNeLat=None, oNeLng=None):
        now_date = datetime.utcnow()
        # now_secs = date_secs(now_date)
        query = Pokemon.select()
        if not (swLat and swLng and neLat and neLng):
            query = (query
                     .where(Pokemon.disappear_time > now_date)
                     .dicts())
        elif timestamp > 0:
            # If timestamp is known only load modified pokemon.
            query = (query
                     .where(((Pokemon.last_modified > datetime.utcfromtimestamp(timestamp / 1000)) &
                             (Pokemon.disappear_time > now_date)) &
                            ((Pokemon.latitude >= swLat) &
                             (Pokemon.longitude >= swLng) &
                             (Pokemon.latitude <= neLat) &
                             (Pokemon.longitude <= neLng)))
                     .dicts())
        elif oSwLat and oSwLng and oNeLat and oNeLng:
            # Send Pokemon in view but exclude those within old boundaries. Only send newly uncovered Pokemon.
            query = (query
                     .where(((Pokemon.disappear_time > now_date) &
                            (((Pokemon.latitude >= swLat) &
                              (Pokemon.longitude >= swLng) &
                              (Pokemon.latitude <= neLat) &
                              (Pokemon.longitude <= neLng))) &
                            ~((Pokemon.disappear_time > now_date) &
                              (Pokemon.latitude >= oSwLat) &
                              (Pokemon.longitude >= oSwLng) &
                              (Pokemon.latitude <= oNeLat) &
                              (Pokemon.longitude <= oNeLng))))
                     .dicts())
        else:
            query = (Pokemon
                     .select()
                     # add 1 hour buffer to include spawnpoints that persist after tth, like shsh
                     .where((Pokemon.disappear_time > now_date) &
                            (((Pokemon.latitude >= swLat) &
                              (Pokemon.longitude >= swLng) &
                              (Pokemon.latitude <= neLat) &
                              (Pokemon.longitude <= neLng))))
                     .dicts())

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append().
        gc.disable()

        pokemons = []
        for p in list(query):
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
            p['pokemon_rarity'] = get_pokemon_rarity(p['pokemon_id'])
            p['pokemon_types'] = get_pokemon_types(p['pokemon_id'])
            if args.china:
                p['latitude'], p['longitude'] = \
                    transform_from_wgs_to_gcj(p['latitude'], p['longitude'])
            pokemons.append(p)

        # Re-enable the GC.
        gc.enable()

        return pokemons


    @staticmethod
    def get_active_by_eid(eids, perfLimit, swLat, swLng, neLat, neLng, timestamp=0, oSwLat=None, oSwLng=None, oNeLat=None, oNeLng=None):
        now_date = datetime.utcnow()
        # now_secs = date_secs(now_date)
        query = Pokemon.select()

        if not (swLat and swLng and neLat and neLng):
            query = (query
                     .where(Pokemon.disappear_time > now_date &
                            (~(Pokemon.pokemon_id << eids) |
                             ((Pokemon.individual_attack +
                              Pokemon.individual_defense +
                              Pokemon.individual_stamina) / 45.0 * 100 >= perfLimit)))
                     .dicts())
        elif timestamp > 0:
            # If timestamp is known only load modified pokemon.
            query = (query
                     .where(((Pokemon.last_modified > datetime.utcfromtimestamp(timestamp / 1000)) &
                             (Pokemon.disappear_time > now_date)) &
                            ((Pokemon.latitude >= swLat) &
                             (Pokemon.longitude >= swLng) &
                             (Pokemon.latitude <= neLat) &
                             (Pokemon.longitude <= neLng)) &
                            (~(Pokemon.pokemon_id << eids) |
                             ((Pokemon.individual_attack +
                              Pokemon.individual_defense +
                              Pokemon.individual_stamina) / 45.0 * 100 >= perfLimit)))
                     .dicts())
        elif oSwLat and oSwLng and oNeLat and oNeLng:
            # Send Pokemon in view but exclude those within old boundaries. Only send newly uncovered Pokemon.
            query = (query
                     .where((Pokemon.disappear_time > now_date) &
                            ((Pokemon.latitude >= swLat) &
                             (Pokemon.longitude >= swLng) &
                             (Pokemon.latitude <= neLat) &
                             (Pokemon.longitude <= neLng)) &
                            ~((Pokemon.disappear_time > now_date) &
                              (Pokemon.latitude >= oSwLat) &
                              (Pokemon.longitude >= oSwLng) &
                              (Pokemon.latitude <= oNeLat) &
                              (Pokemon.longitude <= oNeLng)) &
                            (~(Pokemon.pokemon_id << eids) |
                             ((Pokemon.individual_attack +
                              Pokemon.individual_defense +
                              Pokemon.individual_stamina) / 45.0 * 100 >= perfLimit)))
                     .dicts())
        else:
            query = (Pokemon
                     .select()
                     # add 1 hour buffer to include spawnpoints that persist after tth, like shsh
                     .where((Pokemon.disappear_time > now_date) &
                            ((Pokemon.latitude >= swLat) &
                             (Pokemon.longitude >= swLng) &
                             (Pokemon.latitude <= neLat) &
                             (Pokemon.longitude <= neLng)) &
                            (~(Pokemon.pokemon_id << eids) |
                             ((Pokemon.individual_attack +
                              Pokemon.individual_defense +
                              Pokemon.individual_stamina) / 45.0 * 100 >= perfLimit)))
                     .dicts())

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append().
        gc.disable()

        pokemons = []
        for p in list(query):

            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
            p['pokemon_rarity'] = get_pokemon_rarity(p['pokemon_id'])
            p['pokemon_types'] = get_pokemon_types(p['pokemon_id'])
            if args.china:
                p['latitude'], p['longitude'] = \
                    transform_from_wgs_to_gcj(p['latitude'], p['longitude'])
            pokemons.append(p)

        # Re-enable the GC.
        gc.enable()

        return pokemons


    @staticmethod
    def get_active_by_id(ids, swLat, swLng, neLat, neLng):
        if not (swLat and swLng and neLat and neLng):
            query = (Pokemon
                     .select()
                     .where((Pokemon.pokemon_id << ids) &
                            (Pokemon.disappear_time > datetime.utcnow()))
                     .dicts())
        else:
            query = (Pokemon
                     .select()
                     .where((Pokemon.pokemon_id << ids) &
                            (Pokemon.disappear_time > datetime.utcnow()) &
                            (Pokemon.latitude >= swLat) &
                            (Pokemon.longitude >= swLng) &
                            (Pokemon.latitude <= neLat) &
                            (Pokemon.longitude <= neLng))
                     .dicts())

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append().
        gc.disable()

        pokemons = []
        for p in query:
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
            p['pokemon_rarity'] = get_pokemon_rarity(p['pokemon_id'])
            p['pokemon_types'] = get_pokemon_types(p['pokemon_id'])
            if args.china:
                p['latitude'], p['longitude'] = \
                    transform_from_wgs_to_gcj(p['latitude'], p['longitude'])
            pokemons.append(p)

        # Re-enable the GC.
        gc.enable()

        return pokemons

    @classmethod
    @cached(cache)
    def get_seen(cls, timediff):
        if timediff:
            timediff = datetime.utcnow() - timediff
        pokemon_count_query = (Pokemon
                               .select(Pokemon.pokemon_id,
                                       fn.COUNT(Pokemon.pokemon_id).alias('count'),
                                       fn.MAX(Pokemon.disappear_time).alias('lastappeared')
                                       )
                               .where(Pokemon.disappear_time > timediff)
                               .group_by(Pokemon.pokemon_id)
                               .alias('counttable')
                               )
        query = (Pokemon
                 .select(Pokemon.pokemon_id,
                         Pokemon.disappear_time,
                         Pokemon.latitude,
                         Pokemon.longitude,
                         pokemon_count_query.c.count)
                 .join(pokemon_count_query, on=(Pokemon.pokemon_id == pokemon_count_query.c.pokemon_id))
                 .distinct()
                 .where(Pokemon.disappear_time == pokemon_count_query.c.lastappeared)
                 .dicts()
                 )

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append().
        gc.disable()

        pokemons = []
        total = 0
        for p in query:
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
            pokemons.append(p)
            total += p['count']

        # Re-enable the GC.
        gc.enable()

        return {'pokemon': pokemons, 'total': total}

    @classmethod
    def get_appearances(cls, pokemon_id, timediff):
        '''
        :param pokemon_id: id of pokemon that we need appearances for
        :param timediff: limiting period of the selection
        :return: list of  pokemon  appearances over a selected period
        '''
        if timediff:
            timediff = datetime.utcnow() - timediff
        query = (Pokemon
                 .select(Pokemon.latitude, Pokemon.longitude, Pokemon.pokemon_id, fn.Count(Pokemon.spawnpoint_id).alias('count'), Pokemon.spawnpoint_id)
                 .where((Pokemon.pokemon_id == pokemon_id) &
                        (Pokemon.disappear_time > timediff)
                        )
                 .group_by(Pokemon.latitude, Pokemon.longitude, Pokemon.pokemon_id, Pokemon.spawnpoint_id)
                 .dicts()
                 )

        return list(query)

    @classmethod
    def get_appearances_times_by_spawnpoint(cls, pokemon_id, spawnpoint_id, timediff):
        '''
        :param pokemon_id: id of pokemon that we need appearances times for
        :param spawnpoint_id: spawnpoing id we need appearances times for
        :param timediff: limiting period of the selection
        :return: list of time appearances over a selected period
        '''
        if timediff:
            timediff = datetime.utcnow() - timediff
        query = (Pokemon
                 .select(Pokemon.disappear_time)
                 .where((Pokemon.pokemon_id == pokemon_id) &
                        (Pokemon.spawnpoint_id == spawnpoint_id) &
                        (Pokemon.disappear_time > timediff)
                        )
                 .order_by(Pokemon.disappear_time.asc())
                 .tuples()
                 )

        return list(itertools.chain(*query))

    @classmethod
    def get_spawn_time(cls, disappear_time):
        return (disappear_time + 1800) % 3600

    @classmethod
    def get_spawnpoints(cls, swLat, swLng, neLat, neLng, timestamp=0, oSwLat=None, oSwLng=None, oNeLat=None, oNeLng=None):
        query = Pokemon.select(Pokemon.latitude, Pokemon.longitude, Pokemon.spawnpoint_id, (date_secs(Pokemon.disappear_time)).alias('time'), fn.Count(Pokemon.spawnpoint_id).alias('count'))

        if timestamp > 0:
            query = (query
                     .where(((Pokemon.last_modified > datetime.utcfromtimestamp(timestamp / 1000))) &
                            ((Pokemon.latitude >= swLat) &
                             (Pokemon.longitude >= swLng) &
                             (Pokemon.latitude <= neLat) &
                             (Pokemon.longitude <= neLng)))
                     .dicts())
        elif oSwLat and oSwLng and oNeLat and oNeLng:
            # Send spawnpoints in view but exclude those within old boundaries. Only send newly uncovered spawnpoints.
            query = (query
                     .where((((Pokemon.latitude >= swLat) &
                              (Pokemon.longitude >= swLng) &
                              (Pokemon.latitude <= neLat) &
                              (Pokemon.longitude <= neLng))) &
                            ~((Pokemon.latitude >= oSwLat) &
                              (Pokemon.longitude >= oSwLng) &
                              (Pokemon.latitude <= oNeLat) &
                              (Pokemon.longitude <= oNeLng)))
                     .dicts())
        elif swLat and swLng and neLat and neLng:
            query = (query
                     .where((Pokemon.latitude <= neLat) &
                            (Pokemon.latitude >= swLat) &
                            (Pokemon.longitude >= swLng) &
                            (Pokemon.longitude <= neLng)
                            ))

        query = query.group_by(Pokemon.latitude, Pokemon.longitude, Pokemon.spawnpoint_id, SQL('time'))

        queryDict = query.dicts()
        spawnpoints = {}

        for sp in queryDict:
            key = sp['spawnpoint_id']
            disappear_time = cls.get_spawn_time(sp.pop('time'))
            count = int(sp['count'])

            if key not in spawnpoints:
                spawnpoints[key] = sp
            else:
                spawnpoints[key]['special'] = True

            if 'time' not in spawnpoints[key] or count >= spawnpoints[key]['count']:
                spawnpoints[key]['time'] = disappear_time
                spawnpoints[key]['count'] = count

        for sp in spawnpoints.values():
            del sp['count']

        return list(spawnpoints.values())

    @classmethod
    def get_spawnpoints_in_hex(cls, center, steps):
        log.info('Finding spawn points {} steps away'.format(steps))

        n, e, s, w = hex_bounds(center, steps)

        query = (Pokemon
                 .select(Pokemon.latitude.alias('lat'),
                         Pokemon.longitude.alias('lng'),
                         (date_secs(Pokemon.disappear_time)).alias('time'),
                         Pokemon.spawnpoint_id
                         ))
        query = (query.where((Pokemon.latitude <= n) &
                             (Pokemon.latitude >= s) &
                             (Pokemon.longitude >= w) &
                             (Pokemon.longitude <= e)
                             ))
        # Sqlite doesn't support distinct on columns.
        if args.db_type == 'mysql':
            query = query.distinct(Pokemon.spawnpoint_id)
        else:
            query = query.group_by(Pokemon.spawnpoint_id)

        s = list(query.dicts())

        # The distance between scan circles of radius 70 in a hex is 121.2436
        # steps - 1 to account for the center circle then add 70 for the edge.
        step_distance = ((steps - 1) * 121.2436) + 70
        # Compare spawnpoint list to a circle with radius steps * 120.
        # Uses the direct geopy distance between the center and the spawnpoint.
        filtered = []

        for idx, sp in enumerate(s):
            if geopy.distance.distance(center, (sp['lat'], sp['lng'])).meters <= step_distance:
                filtered.append(s[idx])

        # At this point, 'time' is DISAPPEARANCE time, we're going to morph it to APPEARANCE time.
        for location in filtered:
            # examples: time    shifted
            #           0       (   0 + 2700) = 2700 % 3600 = 2700 (0th minute to 45th minute, 15 minutes prior to appearance as time wraps around the hour.)
            #           1800    (1800 + 2700) = 4500 % 3600 =  900 (30th minute, moved to arrive at 15th minute.)
            # todo: this DOES NOT ACCOUNT for pokemons that appear sooner and live longer, but you'll _always_ have at least 15 minutes, so it works well enough.
            location['time'] = cls.get_spawn_time(location['time'])

        return filtered


class Pokestop(BaseModel):
    pokestop_id = CharField(primary_key=True, max_length=50)
    enabled = BooleanField()
    latitude = DoubleField()
    longitude = DoubleField()
    last_modified = DateTimeField(index=True)
    lure_expiration = DateTimeField(null=True, index=True)
    active_fort_modifier = CharField(max_length=50, null=True)
    last_updated = DateTimeField(null=True, index=True, default=datetime.utcnow)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)

    @staticmethod
    def get_stops(swLat, swLng, neLat, neLng, timestamp=0, oSwLat=None, oSwLng=None, oNeLat=None, oNeLng=None, lured=False):

        query = Pokestop.select(Pokestop.active_fort_modifier, Pokestop.enabled, Pokestop.latitude, Pokestop.longitude, Pokestop.last_modified, Pokestop.lure_expiration, Pokestop.pokestop_id)

        if not (swLat and swLng and neLat and neLng):
            query = (query
                     .dicts())
        elif timestamp > 0:
            query = (query
                     .where(((Pokestop.last_updated > datetime.utcfromtimestamp(timestamp / 1000))) &
                            (Pokestop.latitude >= swLat) &
                            (Pokestop.longitude >= swLng) &
                            (Pokestop.latitude <= neLat) &
                            (Pokestop.longitude <= neLng))
                     .dicts())
        elif oSwLat and oSwLng and oNeLat and oNeLng and lured:
            query = (query
                     .where((((Pokestop.latitude >= swLat) &
                              (Pokestop.longitude >= swLng) &
                              (Pokestop.latitude <= neLat) &
                              (Pokestop.longitude <= neLng)) &
                             (Pokestop.active_fort_modifier.is_null(False))) &
                            ~((Pokestop.latitude >= oSwLat) &
                              (Pokestop.longitude >= oSwLng) &
                              (Pokestop.latitude <= oNeLat) &
                              (Pokestop.longitude <= oNeLng)) &
                             (Pokestop.active_fort_modifier.is_null(False)))
                     .dicts())
        elif oSwLat and oSwLng and oNeLat and oNeLng:
            # Send stops in view but exclude those within old boundaries. Only send newly uncovered stops.
            query = (query
                     .where(((Pokestop.latitude >= swLat) &
                             (Pokestop.longitude >= swLng) &
                             (Pokestop.latitude <= neLat) &
                             (Pokestop.longitude <= neLng)) &
                            ~((Pokestop.latitude >= oSwLat) &
                              (Pokestop.longitude >= oSwLng) &
                              (Pokestop.latitude <= oNeLat) &
                              (Pokestop.longitude <= oNeLng)))
                     .dicts())
        elif lured:
            query = (query
                     .where(((Pokestop.last_updated > datetime.utcfromtimestamp(timestamp / 1000))) &
                            ((Pokestop.latitude >= swLat) &
                             (Pokestop.longitude >= swLng) &
                             (Pokestop.latitude <= neLat) &
                             (Pokestop.longitude <= neLng)) &
                            (Pokestop.active_fort_modifier.is_null(False)))
                     .dicts())

        else:
            query = (query
                     .where((Pokestop.latitude >= swLat) &
                            (Pokestop.longitude >= swLng) &
                            (Pokestop.latitude <= neLat) &
                            (Pokestop.longitude <= neLng))
                     .dicts())

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append().
        gc.disable()

        pokestops = []
        for p in query:
            if args.china:
                p['latitude'], p['longitude'] = \
                    transform_from_wgs_to_gcj(p['latitude'], p['longitude'])
            pokestops.append(p)

        # Re-enable the GC.
        gc.enable()

        return pokestops


class Gym(BaseModel):
    UNCONTESTED = 0
    TEAM_MYSTIC = 1
    TEAM_VALOR = 2
    TEAM_INSTINCT = 3

    gym_id = CharField(primary_key=True, max_length=50)
    team_id = IntegerField()
    guard_pokemon_id = IntegerField()
    gym_points = IntegerField()
    enabled = BooleanField()
    latitude = DoubleField()
    longitude = DoubleField()
    last_modified = DateTimeField(index=True)
    last_scanned = DateTimeField(default=datetime.utcnow)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)

    @staticmethod
    def get_gyms(swLat, swLng, neLat, neLng, timestamp=0, oSwLat=None, oSwLng=None, oNeLat=None, oNeLng=None):
        if not (swLat and swLng and neLat and neLng):
            results = (Gym
                       .select()
                       .dicts())
        elif timestamp > 0:
            # If timestamp is known only send last scanned Gyms.
            results = (Gym
                       .select()
                       .where(((Gym.last_scanned > datetime.utcfromtimestamp(timestamp / 1000)) &
                              (Gym.latitude >= swLat) &
                              (Gym.longitude >= swLng) &
                              (Gym.latitude <= neLat) &
                              (Gym.longitude <= neLng)))
                       .dicts())
        elif oSwLat and oSwLng and oNeLat and oNeLng:
            # Send gyms in view but exclude those within old boundaries. Only send newly uncovered gyms.
            results = (Gym
                       .select()
                       .where(((Gym.latitude >= swLat) &
                               (Gym.longitude >= swLng) &
                               (Gym.latitude <= neLat) &
                               (Gym.longitude <= neLng)) &
                              ~((Gym.latitude >= oSwLat) &
                                (Gym.longitude >= oSwLng) &
                                (Gym.latitude <= oNeLat) &
                                (Gym.longitude <= oNeLng)))
                       .dicts())

        else:
            results = (Gym
                       .select()
                       .where((Gym.latitude >= swLat) &
                              (Gym.longitude >= swLng) &
                              (Gym.latitude <= neLat) &
                              (Gym.longitude <= neLng))
                       .dicts())

        # Performance: Disable the garbage collector prior to creating a (potentially) large dict with append().
        gc.disable()

        gyms = {}
        gym_ids = []
        for g in results:
            g['name'] = None
            g['pokemon'] = []
            gyms[g['gym_id']] = g
            gym_ids.append(g['gym_id'])

        if len(gym_ids) > 0:
            pokemon = (GymMember
                       .select(
                           GymMember.gym_id,
                           GymPokemon.cp.alias('pokemon_cp'),
                           GymPokemon.pokemon_id,
                           Trainer.name.alias('trainer_name'),
                           Trainer.level.alias('trainer_level'))
                       .join(Gym, on=(GymMember.gym_id == Gym.gym_id))
                       .join(GymPokemon, on=(GymMember.pokemon_uid == GymPokemon.pokemon_uid))
                       .join(Trainer, on=(GymPokemon.trainer_name == Trainer.name))
                       .where(GymMember.gym_id << gym_ids)
                       .where(GymMember.last_scanned > Gym.last_modified)
                       .order_by(GymMember.gym_id, GymPokemon.cp)
                       .dicts())

            for p in pokemon:
                p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
                gyms[p['gym_id']]['pokemon'].append(p)

            details = (GymDetails
                       .select(
                           GymDetails.gym_id,
                           GymDetails.name)
                       .where(GymDetails.gym_id << gym_ids)
                       .dicts())

            for d in details:
                gyms[d['gym_id']]['name'] = d['name']

        # Re-enable the GC.
        gc.enable()

        return gyms

    @staticmethod
    def get_gym(id):
        result = (Gym
                  .select(Gym.gym_id,
                          Gym.team_id,
                          GymDetails.name,
                          GymDetails.description,
                          Gym.guard_pokemon_id,
                          Gym.gym_points,
                          Gym.latitude,
                          Gym.longitude,
                          Gym.last_modified,
                          Gym.last_scanned)
                  .join(GymDetails, JOIN.LEFT_OUTER, on=(Gym.gym_id == GymDetails.gym_id))
                  .where(Gym.gym_id == id)
                  .dicts()
                  .get())

        result['guard_pokemon_name'] = get_pokemon_name(result['guard_pokemon_id']) if result['guard_pokemon_id'] else ''
        result['pokemon'] = []

        pokemon = (GymMember
                   .select(GymPokemon.cp.alias('pokemon_cp'),
                           GymPokemon.pokemon_id,
                           GymPokemon.pokemon_uid,
                           GymPokemon.move_1,
                           GymPokemon.move_2,
                           GymPokemon.iv_attack,
                           GymPokemon.iv_defense,
                           GymPokemon.iv_stamina,
                           Trainer.name.alias('trainer_name'),
                           Trainer.level.alias('trainer_level'))
                   .join(Gym, on=(GymMember.gym_id == Gym.gym_id))
                   .join(GymPokemon, on=(GymMember.pokemon_uid == GymPokemon.pokemon_uid))
                   .join(Trainer, on=(GymPokemon.trainer_name == Trainer.name))
                   .where(GymMember.gym_id == id)
                   .where(GymMember.last_scanned > Gym.last_modified)
                   .order_by(GymPokemon.cp.desc())
                   .dicts())

        for p in pokemon:
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])

            p['move_1_name'] = get_move_name(p['move_1'])
            p['move_1_damage'] = get_move_damage(p['move_1'])
            p['move_1_energy'] = get_move_energy(p['move_1'])
            p['move_1_type'] = get_move_type(p['move_1'])

            p['move_2_name'] = get_move_name(p['move_2'])
            p['move_2_damage'] = get_move_damage(p['move_2'])
            p['move_2_energy'] = get_move_energy(p['move_2'])
            p['move_2_type'] = get_move_type(p['move_2'])

            result['pokemon'].append(p)

        return result


class ScannedLocation(BaseModel):
    cellid = CharField(primary_key=True, max_length=50)
    latitude = DoubleField()
    longitude = DoubleField()
    last_modified = DateTimeField(index=True, default=datetime.utcnow, null=True)
    # marked true when all five bands have been completed
    done = BooleanField(default=False)

    # Five scans/hour is required to catch all spawns
    # Each scan must be at least 12 minutes from the previous check,
    # with a 2 minute window during which the scan can be done

    # default of -1 is for bands not yet scanned
    band1 = IntegerField(default=-1)
    band2 = IntegerField(default=-1)
    band3 = IntegerField(default=-1)
    band4 = IntegerField(default=-1)
    band5 = IntegerField(default=-1)

    # midpoint is the center of the bands relative to band 1
    # e.g., if band 1 is 10.4 min, and band 4 is 34.0 min, midpoint is -0.2 min in minsec
    # extra 10 seconds in case of delay in recording now time
    midpoint = IntegerField(default=0)

    # width is how wide the valid window is. Default is 0, max is 2 min
    # e.g., if band 1 is 10.4 min, and band 4 is 34.0 min, midpoint is 0.4 min in minsec
    width = IntegerField(default=0)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)
        constraints = [Check('band1 >= -1'), Check('band1 < 3600'),
                       Check('band2 >= -1'), Check('band2 < 3600'),
                       Check('band3 >= -1'), Check('band3 < 3600'),
                       Check('band4 >= -1'), Check('band4 < 3600'),
                       Check('band5 >= -1'), Check('band5 < 3600'),
                       Check('midpoint >= -130'), Check('midpoint <= 130'),
                       Check('width >= 0'), Check('width <= 130')]

    @staticmethod
    def get_recent(swLat, swLng, neLat, neLng, timestamp=0, oSwLat=None, oSwLng=None, oNeLat=None, oNeLng=None):
        activeTime = (datetime.utcnow() - timedelta(minutes=15))
        if timestamp > 0:
            query = (ScannedLocation
                     .select()
                     .where(((ScannedLocation.last_modified >= datetime.utcfromtimestamp(timestamp / 1000))) &
                            (ScannedLocation.latitude >= swLat) &
                            (ScannedLocation.longitude >= swLng) &
                            (ScannedLocation.latitude <= neLat) &
                            (ScannedLocation.longitude <= neLng))
                     .dicts())
        elif oSwLat and oSwLng and oNeLat and oNeLng:
            # Send scannedlocations in view but exclude those within old boundaries. Only send newly uncovered scannedlocations.
            query = (ScannedLocation
                     .select()
                     .where((((ScannedLocation.last_modified >= activeTime)) &
                             (ScannedLocation.latitude >= swLat) &
                             (ScannedLocation.longitude >= swLng) &
                             (ScannedLocation.latitude <= neLat) &
                             (ScannedLocation.longitude <= neLng)) &
                            ~(((ScannedLocation.last_modified >= activeTime)) &
                              (ScannedLocation.latitude >= oSwLat) &
                              (ScannedLocation.longitude >= oSwLng) &
                              (ScannedLocation.latitude <= oNeLat) &
                              (ScannedLocation.longitude <= oNeLng)))
                     .dicts())
        else:
            query = (ScannedLocation
                     .select()
                     .where((ScannedLocation.last_modified >= activeTime) &
                            (ScannedLocation.latitude >= swLat) &
                            (ScannedLocation.longitude >= swLng) &
                            (ScannedLocation.latitude <= neLat) &
                            (ScannedLocation.longitude <= neLng))
                     .order_by(ScannedLocation.last_modified.asc())
                     .dicts())

        return list(query)

    # DB format of a new location
    @staticmethod
    def new_loc(loc):
        return {'cellid': cellid(loc),
                'latitude': loc[0],
                'longitude': loc[1],
                'done': False,
                'band1': -1,
                'band2': -1,
                'band3': -1,
                'band4': -1,
                'band5': -1,
                'width': 0,
                'midpoint': 0,
                'last_modified': None}

    # Used to update bands
    @staticmethod
    def db_format(scan, band, nowms):
        scan.update({'band' + str(band): nowms})
        scan['done'] = reduce(lambda x, y: x and (scan['band' + str(y)] > -1), range(1, 6), True)
        return scan

    # Shorthand helper for DB dict
    @staticmethod
    def _q_init(scan, start, end, kind, sp_id=None):
        return {'loc': scan['loc'], 'kind': kind, 'start': start, 'end': end, 'step': scan['step'], 'sp': sp_id}

    # return value of a particular scan from loc, or default dict if not found
    @classmethod
    def get_by_loc(cls, loc):
        query = (cls
                 .select()
                 .where((ScannedLocation.latitude == loc[0]) &
                        (ScannedLocation.longitude == loc[1]))
                 .dicts())

        return query[0] if len(list(query)) else cls.new_loc(loc)

    # Check if spawn points in a list are in any of the existing spannedlocation records
    # Otherwise, search through the spawn point list, and update scan_spawn_point dict for DB bulk upserting
    @classmethod
    def link_spawn_points(cls, scans, initial, spawn_points, distance, scan_spawn_point, force=False):
        for cell, scan in scans.iteritems():
            if initial[cell]['done'] and not force:
                continue

            for sp in spawn_points:
                if in_radius((sp['latitude'], sp['longitude']), scan['loc'], distance):
                    scan_spawn_point[cell + sp['id']] = {'spawnpoint': sp['id'],
                                                         'scannedlocation': cell}

    # return list of dicts for upcoming valid band times
    @classmethod
    def linked_spawn_points(cls, cell):

        # unable to use a normal join, since MySQL produces foreignkey constraint errors when
        # trying to upsert fields that are foreignkeys on another table

        query = (SpawnPoint
                 .select()
                 .join(ScanSpawnPoint)
                 .join(cls)
                 .where(cls.cellid == cell).dicts())

        return list(query)

    # return list of dicts for upcoming valid band times
    @staticmethod
    def visible_forts(step_location):
        distance = 0.9
        n, e, s, w = hex_bounds(step_location, radius=distance * 1000)
        for g in Gym.get_gyms(s, w, n, e).values():
            if in_radius((g['latitude'], g['longitude']), step_location, distance):
                return True

        for g in Pokestop.get_stops(s, w, n, e):
            if in_radius((g['latitude'], g['longitude']), step_location, distance):
                return True

        return False

    # return list of dicts for upcoming valid band times
    @classmethod
    def get_times(cls, scan, now_date):
        s = cls.get_by_loc(scan['loc'])
        if s['done']:
            return []

        max = 3600 * 2 + 250  # greater than maximum possible value
        min = {'end': max}

        nowms = date_secs(now_date)
        if s['band1'] == -1:
            return [cls._q_init(scan, nowms, nowms + 3599, 'band')]

        # Find next window
        basems = s['band1']
        for i in range(2, 6):
            ms = s['band' + str(i)]

            # skip bands already done
            if ms > -1:
                continue

            radius = 120 - s['width'] / 2
            end = (basems + s['midpoint'] + radius + (i - 1) * 720 - 10) % 3600
            end = end if end >= nowms else end + 3600

            if end < min['end']:
                min = cls._q_init(scan, end - radius * 2 + 10, end, 'band')

        return [min] if min['end'] < max else []

    # Checks if now falls within an unfilled band for a scanned location
    # Returns the updated scan location dict
    @classmethod
    def update_band(cls, scan):
        now_date = datetime.utcnow()
        scan['last_modified'] = now_date

        if scan['done']:
            return scan

        now_secs = date_secs(now_date)
        if scan['band1'] == -1:
            return cls.db_format(scan, 1, now_secs)

        # calc if number falls in band with remaining points
        basems = scan['band1']
        delta = (now_secs - basems - scan['midpoint']) % 3600
        band = int(round(delta / 12 / 60.0) % 5) + 1

        # Check if that band already filled
        if scan['band' + str(band)] > -1:
            return scan

        # Check if this result falls within the band 2 min window
        offset = (delta + 1080) % 720 - 360
        if abs(offset) > 120 - scan['width'] / 2:
            return scan

        # find band midpoint/width
        scan = cls.db_format(scan, band, now_secs)
        bts = [scan['band' + str(i)] for i in range(1, 6)]
        bts = filter(lambda ms: ms > -1, bts)
        bts_delta = map(lambda ms: (ms - basems) % 3600, bts)
        bts_offsets = map(lambda ms: (ms + 1080) % 720 - 360, bts_delta)
        min_scan = min(bts_offsets)
        max_scan = max(bts_offsets)
        scan['width'] = max_scan - min_scan
        scan['midpoint'] = (max_scan + min_scan) / 2

        return scan

    @classmethod
    def bands_filled(cls, locations):
        filled = 0
        for e in locations:
            sl = cls.get_by_loc(e[1])
            bands = [sl['band' + str(i)] for i in range(1, 6)]
            filled += reduce(lambda x, y: x + (y > -1), bands, 0)

        return filled

    @classmethod
    def reset_bands(cls, scan_loc):
        scan_loc['done'] = False
        scan_loc['last_modified'] = datetime.utcnow()
        for i in range(1, 6):
            scan_loc['band' + str(i)] = -1

    @classmethod
    def select_in_hex(cls, center, steps):
        # should be a way to delegate this to SpawnPoint.select_in_hex, but w/e

        R = 6378.1  # km radius of the earth
        hdist = ((steps * 120.0) - 50.0) / 1000.0
        n, e, s, w = hex_bounds(center, steps)

        # get all spawns in that box
        sp = list(cls
                  .select()
                  .where((cls.latitude <= n) &
                         (cls.latitude >= s) &
                         (cls.longitude >= w) &
                         (cls.longitude <= e))
                  .dicts())

        # for each spawn work out if it is in the hex (clipping the diagonals)
        in_hex = []
        for spawn in sp:
            # get the offset from the center of each spawn in km
            offset = [math.radians(spawn['latitude'] - center[0]) * R,
                      math.radians(spawn['longitude'] - center[1]) * (R * math.cos(math.radians(center[0])))]
            # check agains the 4 lines that make up the diagonals
            if (offset[1] + (offset[0] * 0.5)) > hdist:  # too far ne
                continue
            if (offset[1] - (offset[0] * 0.5)) > hdist:  # too far se
                continue
            if ((offset[0] * 0.5) - offset[1]) > hdist:  # too far nw
                continue
            if ((0 - offset[1]) - (offset[0] * 0.5)) > hdist:  # too far sw
                continue
            # if it gets to here its  a good spawn
            in_hex.append(spawn)
        return in_hex


class MainWorker(BaseModel):
    worker_name = CharField(primary_key=True, max_length=50)
    message = CharField()
    method = CharField(max_length=50)
    last_modified = DateTimeField(index=True)


class WorkerStatus(BaseModel):
    username = CharField(primary_key=True, max_length=50)
    worker_name = CharField(index=True, max_length=50)
    success = IntegerField()
    fail = IntegerField()
    no_items = IntegerField()
    skip = IntegerField()
    last_modified = DateTimeField(index=True)
    message = CharField(max_length=255)
    last_scan_date = DateTimeField(index=True)
    latitude = DoubleField(null=True)
    longitude = DoubleField(null=True)

    @staticmethod
    def db_format(status, name='status_worker_db'):
        status['worker_name'] = status.get('worker_name', name)
        return {'username': status['username'],
                'worker_name': status['worker_name'],
                'success': status['success'],
                'fail': status['fail'],
                'no_items': status['noitems'],
                'skip': status['skip'],
                'last_modified': datetime.utcnow(),
                'message': status['message'],
                'last_scan_date': status.get('last_scan_date', datetime.utcnow()),
                'latitude': status.get('latitude', None),
                'longitude': status.get('longitude', None)}

    @staticmethod
    def get_recent():
        query = (WorkerStatus
                 .select()
                 .where((WorkerStatus.last_modified >=
                        (datetime.utcnow() - timedelta(minutes=5))))
                 .order_by(WorkerStatus.username)
                 .dicts())

        status = []
        for s in query:
            status.append(s)

        return status

    @staticmethod
    def get_worker(username, loc=False):
        query = (WorkerStatus
                 .select()
                 .where((WorkerStatus.username == username))
                 .dicts())

        # Sometimes is appears peewee is slow to load, and and this produces an Exception
        # Retry after a second to give peewee time to load
        while True:
            try:
                result = query[0] if len(query) else {
                    'username': username,
                    'success': 0,
                    'fail': 0,
                    'no_items': 0,
                    'skip': 0,
                    'last_modified': datetime.utcnow(),
                    'message': 'New account {} loaded'.format(username),
                    'last_scan_date': datetime.utcnow(),
                    'latitude': loc[0] if loc else None,
                    'longitude': loc[1] if loc else None
                }
                break
            except Exception as e:
                log.error('Exception in get_worker under account {} Exception message: {}'.format(username, e))
                traceback.print_exc(file=sys.stdout)
                time.sleep(1)

        return result


class SpawnPoint(BaseModel):
    id = CharField(primary_key=True, max_length=50)
    latitude = DoubleField()
    longitude = DoubleField()
    last_scanned = DateTimeField(index=True)
    # kind gives the four quartiles of the spawn, as 's' for seen or 'h' for hidden
    # for example, a 30 minute spawn is 'hhss'
    kind = CharField(max_length=4, default='hhhs')

    # links shows whether a pokemon encounter id changes between quartiles or stays the same
    # both 1x45 and 1x60h3 have the kind of 'sssh', but the different links shows when the
    # encounter id changes
    # same encounter id is shared between two quartiles, links shows a '+',
    # a different encounter id between two quartiles is a '-'
    # For the hidden times, an 'h' is used. Until determined, '?' is used.
    # Note index is shifted by a half. links[0] is the link between kind[0] and kind[1],
    # and so on. links[3] is the link between kind[3] and kind[0]
    links = CharField(max_length=4, default='????')

    # count consecutive times spawn should have been seen, but wasn't
    # if too high, will not be scheduled for review, and treated as inactive
    missed_count = IntegerField(default=0)

    # next 2 fields are to narrow down on the valid TTH window
    # seconds after the hour of the latest pokemon seen time within the hour
    latest_seen = IntegerField()

    # seconds after the hour of the earliest time wasn't seen after an appearance
    earliest_unseen = IntegerField()

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)
        constraints = [Check('earliest_unseen >= 0'), Check('earliest_unseen < 3600'),
                       Check('latest_seen >= 0'), Check('latest_seen < 3600')]

    # Returns the spawn point dict from ID, or a new dict if not found
    @classmethod
    def get_by_id(cls, id, latitude=0, longitude=0):
        query = (cls
                 .select()
                 .where(cls.id == id)
                 .dicts())

        return query[0] if query else {
            'id': id,
            'latitude': latitude,
            'longitude': longitude,
            'last_scanned': None,  # Null value used as new flag
            'kind': 'hhhs',
            'links': '????',
            'missed_count': 0,
            'latest_seen': None,
            'earliest_unseen': None

        }

    # Confirm if tth has been found
    @staticmethod
    def tth_found(sp):
        # fully indentified if no '?' in links and latest seen == earliest seen
        return sp['latest_seen'] == sp['earliest_unseen']

    # return [start, end] in seconds after the hour for the spawn, despawn time of a spawnpoint
    @classmethod
    def start_end(cls, sp, spawn_delay=0, links=False):
        links_arg = links
        links = links if links else str(sp['links'])

        if links == '????':  # clean up for old data
            links = str(sp['kind'].replace('s', '?'))

        # make some assumptions if link not fully identified
        if links.count('-') == 0:
            links = links[:-1] + '-'

        links = links.replace('?', '+')

        links = links[:-1] + '-'
        plus_or_minus = links.index('+') if links.count('+') else links.index('-')
        start = sp['earliest_unseen'] - (4 - plus_or_minus) * 900 + spawn_delay
        no_tth_adjust = 60 if not links_arg and not cls.tth_found(sp) else 0
        end = sp['latest_seen'] - (3 - links.index('-')) * 900 + no_tth_adjust
        return [start % 3600, end % 3600]

    # Return a list of dicts with the next spawn times
    @classmethod
    def get_times(cls, cell, scan, now_date, scan_delay):
        l = []
        now_secs = date_secs(now_date)
        for sp in ScannedLocation.linked_spawn_points(cell):

            if sp['missed_count'] > 5:
                continue

            endpoints = SpawnPoint.start_end(sp, scan_delay)
            cls.add_if_not_scanned('spawn', l, sp, scan, endpoints[0], endpoints[1], now_date, now_secs)

            # check to see if still searching for valid TTH
            if cls.tth_found(sp):
                continue

            # add a spawnpoint check between latest seen and earliest seen
            start = sp['latest_seen'] + scan_delay
            end = sp['earliest_unseen']

            cls.add_if_not_scanned('TTH', l, sp, scan, start, end, now_date, now_secs)

        return l

    @classmethod
    def add_if_not_scanned(cls, kind, l, sp, scan, start, end, now_date, now_secs):
        # make sure later than now_secs
        while end < now_secs:
            start, end = start + 3600, end + 3600

        # ensure start before end
        while start > end:
            start -= 3600

        if (now_date - cls.get_by_id(sp['id'])['last_scanned']).total_seconds() > now_secs - start:
            l.append(ScannedLocation._q_init(scan, start, end, kind, sp['id']))

    # given seconds after the hour and a spawnpoint dict, return which quartile of the
    # spawnpoint the secs falls in
    @staticmethod
    def get_quartile(secs, sp):
        return int(((secs - sp['earliest_unseen'] + 15 * 60 + 3600 - 1) % 3600) / 15 / 60)

    @classmethod
    def select_in_hex(cls, center, steps):
        R = 6378.1  # km radius of the earth
        hdist = ((steps * 120.0) - 50.0) / 1000.0
        n, e, s, w = hex_bounds(center, steps)

        # get all spawns in that box
        sp = list(cls
                  .select()
                  .where((cls.latitude <= n) &
                         (cls.latitude >= s) &
                         (cls.longitude >= w) &
                         (cls.longitude <= e))
                  .dicts())

        # for each spawn work out if it is in the hex (clipping the diagonals)
        in_hex = []
        for spawn in sp:
            # get the offset from the center of each spawn in km
            offset = [math.radians(spawn['latitude'] - center[0]) * R,
                      math.radians(spawn['longitude'] - center[1]) * (R * math.cos(math.radians(center[0])))]
            # check agains the 4 lines that make up the diagonals
            if (offset[1] + (offset[0] * 0.5)) > hdist:  # too far ne
                continue
            if (offset[1] - (offset[0] * 0.5)) > hdist:  # too far se
                continue
            if ((offset[0] * 0.5) - offset[1]) > hdist:  # too far nw
                continue
            if ((0 - offset[1]) - (offset[0] * 0.5)) > hdist:  # too far sw
                continue
            # if it gets to here its  a good spawn
            in_hex.append(spawn)
        return in_hex


class ScanSpawnPoint(BaseModel):
    # removing ForeignKeyField due to MSQL issues with upserting rows that are foreignkeys for other tables
    # scannedlocation = ForeignKeyField(ScannedLocation)
    # spawnpoint = ForeignKeyField(SpawnPoint)

    scannedlocation = ForeignKeyField(ScannedLocation, null=True)
    spawnpoint = ForeignKeyField(SpawnPoint, null=True)

    class Meta:
        primary_key = CompositeKey('spawnpoint', 'scannedlocation')


class SpawnpointDetectionData(BaseModel):
    id = CharField(primary_key=True, max_length=54)
    encounter_id = CharField(max_length=54)  # removed ForeignKeyField since it caused MySQL issues
    spawnpoint_id = CharField(max_length=54)  # removed ForeignKeyField since it caused MySQL issues
    scan_time = DateTimeField()
    tth_secs = IntegerField(null=True)

    @staticmethod
    def set_default_earliest_unseen(sp):
        sp['earliest_unseen'] = (sp['latest_seen'] + 14 * 60) % 3600

    @classmethod
    def classify(cls, sp, scan_loc, now_secs, sighting=None):

        # to reduce CPU usage, give an intial reading of 15 min spawns if not done with initial scan of location
        if not scan_loc['done']:
            sp['kind'] = 'hhhs'
            if not sp['earliest_unseen']:
                sp['latest_seen'] = now_secs
                cls.set_default_earliest_unseen(sp)

            elif clock_between(sp['latest_seen'], now_secs, sp['earliest_unseen']):
                sp['latest_seen'] = now_secs

            return

        # get past sightings
        query = list(cls.select()
                        .where(cls.spawnpoint_id == sp['id'])
                        .dicts())

        if sighting:
            query.append(sighting)

        # make a record of links, so we can reset earliest_unseen if it changes
        old_kind = str(sp['kind'])

        # make a sorted list of the seconds after the hour
        seen_secs = sorted(map(lambda x: date_secs(x['scan_time']), query))

        # add the first seen_secs to the end as a clock wrap around
        if seen_secs:
            seen_secs.append(seen_secs[0] + 3600)

        # make a list of gaps between sightings
        gap_list = [seen_secs[i + 1] - seen_secs[i] for i in range(len(seen_secs) - 1)]

        max_gap = max(gap_list)

        # an hour (60 min) minus the largest gap in minutes gives us the duration the spawn was there
        # round up to the nearest 15 min interval for our current best duration guess
        duration = (int((59 - max_gap / 60.0) / 15) + 1) * 15

        # if the second largest gap is larger than 15 minutes, then there are two gaps that are
        # greater than 15 min, so it must be a double-spawn
        if len(gap_list) > 4 and sorted(gap_list)[-2] > 900:
            sp['kind'] = 'hshs'
            sp['links'] = 'h?h?'

        else:
            # convert the duration into a 'hhhs', 'hhss', 'hsss', 'ssss' string accordingly
            # 's' is for seen, 'h' is for hidden
            sp['kind'] = ''.join(['s' if i > (3 - duration / 15) else 'h' for i in range(0, 4)])

        # assume no hidden times
        sp['links'] = sp['kind'].replace('s', '?')

        if sp['kind'] != 'ssss':

            if not sp['earliest_unseen'] or sp['earliest_unseen'] != sp['latest_seen']:

                # new latest seen will be just before max_gap
                sp['latest_seen'] = seen_secs[gap_list.index(max_gap)]

                # if we don't have a earliest_unseen yet or the kind of spawn has changed, reset
                # set to latest_seen + 14 min
                if not sp['earliest_unseen'] or sp['kind'] != old_kind:
                    cls.set_default_earliest_unseen(sp)

            return

        # only ssss spawns from here below

        sp['links'] = '+++-'
        if sp['earliest_unseen'] == sp['latest_seen']:
            return

        # make a sight_list of dicts like
        # {date: first seen time,
        # delta: duration of sighting,
        # same: whether encounter ID was same or different over that time}

        # for 60 min spawns ('ssss'), the largest gap doesn't give the earliest spawn point,
        # because a pokemon is always there
        # use the union of all intervals where the same encounter ID was seen to find the latest_seen
        # If a different encounter ID was seen, then the complement of that interval was the same
        # ID, so union that complement as well

        sight_list = [{'date': query[i]['scan_time'],
                       'delta': query[i + 1]['scan_time'] - query[i]['scan_time'],
                       'same': query[i + 1]['encounter_id'] == query[i]['encounter_id']}
                      for i in range(len(query) - 1)
                      if query[i + 1]['scan_time'] - query[i]['scan_time'] < timedelta(hours=1)]

        start_end_list = []
        for s in sight_list:
            if s['same']:
                # get the seconds past the hour for start and end times
                start = date_secs(s['date'])
                end = (start + int(s['delta'].total_seconds())) % 3600

            else:
                # convert diff range to same range by taking the clock complement
                start = date_secs(s['date'] + s['delta']) % 3600
                end = date_secs(s['date'])

            start_end_list.append([start, end])

        # Take the union of all the ranges
        while True:
            # union is list of unions of ranges with the same encounter id
            union = []
            for start, end in start_end_list:
                if not union:
                    union.append([start, end])
                    continue
                # cycle through all ranges in union, since it might overlap with any of them
                for u in union:
                    if clock_between(u[0], start, u[1]):
                        u[1] = end if not(clock_between(u[0], end, u[1])) else u[1]
                    elif clock_between(u[0], end, u[1]):
                        u[0] = start if not(clock_between(u[0], start, u[1])) else u[0]
                    elif union.count([start, end]) == 0:
                        union.append([start, end])

            # Are no more unions possible?
            if union == start_end_list:
                break
            else:
                start_end_list = union  # Make another pass looking for unions

        # if more than one disparate union, take the largest as our starting point
        union = reduce(lambda x, y: x if (x[1] - x[0]) % 3600 > (y[1] - y[0]) % 3600 else y,
                       union, [0, 3600])
        sp['latest_seen'] = union[1]
        sp['earliest_unseen'] = union[0]
        log.info('1x60: appear %d, despawn %d, duration: %d min', union[0], union[1], ((union[1] - union[0]) % 3600) / 60)

    # expand the seen times for 30 minute spawnpoints based on scans when spawn wasn't there
    # return true if spawnpoint dict changed
    @classmethod
    def unseen(cls, sp, now_secs):

        # return if we already have a tth
        if sp['latest_seen'] == sp['earliest_unseen']:
            return False

        # if now_secs is later than the latest seen return
        if not clock_between(sp['latest_seen'], now_secs, sp['earliest_unseen']):
            return False

        sp['earliest_unseen'] = now_secs

        return True

    # expand a 30 minute spawn with a new seen point based on which endpoint it is closer to
    # return true if sp changed
    @classmethod
    def clock_extend(cls, sp, new_secs):
        # check if this is a new earliest time
        if clock_between(sp['earliest_seen'], new_secs, sp['latest_seen']):
            return False

        # extend earliest or latest seen depending on which is closer to the new point
        if secs_between(new_secs, sp['earliest_seen']) < secs_between(new_secs, sp['latest_seen']):
            sp['earliest_seen'] = new_secs
        else:
            sp['latest_seen'] = new_secs

        return True


class Versions(flaskDb.Model):
    key = CharField()
    val = IntegerField()

    class Meta:
        primary_key = False


class GymMember(BaseModel):
    gym_id = CharField(index=True)
    pokemon_uid = CharField()
    last_scanned = DateTimeField(default=datetime.utcnow)

    class Meta:
        primary_key = False


class GymPokemon(BaseModel):
    pokemon_uid = CharField(primary_key=True, max_length=50)
    pokemon_id = IntegerField()
    cp = IntegerField()
    trainer_name = CharField()
    num_upgrades = IntegerField(null=True)
    move_1 = IntegerField(null=True)
    move_2 = IntegerField(null=True)
    height = FloatField(null=True)
    weight = FloatField(null=True)
    stamina = IntegerField(null=True)
    stamina_max = IntegerField(null=True)
    cp_multiplier = FloatField(null=True)
    additional_cp_multiplier = FloatField(null=True)
    iv_defense = IntegerField(null=True)
    iv_stamina = IntegerField(null=True)
    iv_attack = IntegerField(null=True)
    last_seen = DateTimeField(default=datetime.utcnow)


class Trainer(BaseModel):
    name = CharField(primary_key=True, max_length=50)
    team = IntegerField()
    level = IntegerField()
    last_seen = DateTimeField(default=datetime.utcnow)


class GymDetails(BaseModel):
    gym_id = CharField(primary_key=True, max_length=50)
    name = CharField()
    description = TextField(null=True, default="")
    url = CharField()
    last_scanned = DateTimeField(default=datetime.utcnow)


def hex_bounds(center, steps=None, radius=None):
    # Make a box that is (70m * step_limit * 2) + 70m away from the center point
    # Rationale is that you need to travel
    sp_dist = 0.07 * (2 * steps + 1) if steps else radius
    n = get_new_coords(center, sp_dist, 0)[0]
    e = get_new_coords(center, sp_dist, 90)[1]
    s = get_new_coords(center, sp_dist, 180)[0]
    w = get_new_coords(center, sp_dist, 270)[1]
    return (n, e, s, w)



def db_updater(args, q, db):
    # The forever loop.
    while True:
        try:

            while True:
                try:
                    flaskDb.connect_db()
                    break
                except Exception as e:
                    log.warning('%s... Retrying', e)

            # Loop the queue.
            while True:
                model, data = q.get()
                bulk_upsert(model, data, db)
                q.task_done()
                log.debug('Upserted to %s, %d records (upsert queue remaining: %d)',
                          model.__name__,
                          len(data),
                          q.qsize())
                if q.qsize() > 50:
                    log.warning("DB queue is > 50 (@%d); try increasing --db-threads", q.qsize())

        except Exception as e:
            log.exception('Exception in db_updater: %s', e)


def clean_db_loop(args):
    while True:
        try:
            query = (MainWorker
                     .delete()
                     .where((ScannedLocation.last_modified <
                             (datetime.utcnow() - timedelta(minutes=30)))))
            query.execute()

            query = (WorkerStatus
                     .delete()
                     .where((ScannedLocation.last_modified <
                             (datetime.utcnow() - timedelta(minutes=30)))))
            query.execute()

            # Remove active modifier from expired lured pokestops.
            query = (Pokestop
                     .update(lure_expiration=None, active_fort_modifier=None)
                     .where(Pokestop.lure_expiration < datetime.utcnow()))
            query.execute()

            # If desired, clear old pokemon spawns.
            if args.purge_data > 0:
                query = (Pokemon
                         .delete()
                         .where((Pokemon.disappear_time <
                                (datetime.utcnow() - timedelta(hours=args.purge_data)))))
                query.execute()

            log.info('Regular database cleaning complete')
            time.sleep(60)
        except Exception as e:
            log.exception('Exception in clean_db_loop: %s', e)


def bulk_upsert(cls, data, db):
    num_rows = len(data.values())
    i = 0

    if args.db_type == 'mysql':
        step = 120
    else:
        # SQLite has a default max number of parameters of 999,
        # so we need to limit how many rows we insert for it.
        step = 50

    while i < num_rows:
        log.debug('Inserting items %d to %d', i, min(i + step, num_rows))
        try:
            # Turn off FOREIGN_KEY_CHECKS on MySQL, because it apparently is unable
            # to recognize strings to update unicode keys for foriegn key fields,
            # thus giving lots of foreign key constraint errors
            if args.db_type == 'mysql':
                db.execute_sql('SET FOREIGN_KEY_CHECKS=0;')

            InsertQuery(cls, rows=data.values()[i:min(i + step, num_rows)]).upsert().execute()

            if args.db_type == 'mysql':
                db.execute_sql('SET FOREIGN_KEY_CHECKS=1;')

        except Exception as e:
            # if there is a DB table constraint error, dump the data and don't retry
            # unrecoverable error strings:
            unrecoverable = ['constraint', 'has no attribute', 'peewee.IntegerField object at']
            has_unrecoverable = filter(lambda x: x in str(e), unrecoverable)
            if has_unrecoverable:
                log.warning('%s. Data is:', e)
                log.warning(data.items())
            else:
                log.warning('%s... Retrying', e)
                time.sleep(1)
                continue

        i += step


def create_tables(db):
    db.connect()
    verify_database_schema(db)
    db.create_tables([Pokemon, Pokestop, Gym, ScannedLocation, GymDetails, GymMember, GymPokemon, Trainer, MainWorker, WorkerStatus], safe=True)
    db.close()


def drop_tables(db):
    db.connect()
    db.drop_tables([Pokemon, Pokestop, Gym, ScannedLocation, Versions, GymDetails, GymMember, GymPokemon, Trainer, MainWorker, WorkerStatus, Versions], safe=True)
    db.close()


def verify_database_schema(db):
    if not Versions.table_exists():
        db.create_tables([Versions])

        if ScannedLocation.table_exists():
            # Versions table didn't exist, but there were tables. This must mean the user
            # is coming from a database that existed before we started tracking the schema
            # version. Perform a full upgrade.
            InsertQuery(Versions, {Versions.key: 'schema_version', Versions.val: 0}).execute()
            database_migrate(db, 0)
        else:
            InsertQuery(Versions, {Versions.key: 'schema_version', Versions.val: db_schema_version}).execute()

    else:
        db_ver = Versions.get(Versions.key == 'schema_version').val

        if db_ver < db_schema_version:
            database_migrate(db, db_ver)

        elif db_ver > db_schema_version:
            log.error("Your database version (%i) appears to be newer than the code supports (%i).",
                      db_ver, db_schema_version)
            log.error("Please upgrade your code base or drop all tables in your database.")
            sys.exit(1)


def database_migrate(db, old_ver):
    # Update database schema version.
    Versions.update(val=db_schema_version).where(Versions.key == 'schema_version').execute()

    log.info("Detected database version %i, updating to %i", old_ver, db_schema_version)

    # Perform migrations here.
    migrator = None
    if args.db_type == 'mysql':
        migrator = MySQLMigrator(db)
    else:
        migrator = SqliteMigrator(db)

#   No longer necessary, we're doing this at schema 4 as well.
#    if old_ver < 1:
#        db.drop_tables([ScannedLocation])

    if old_ver < 2:
        migrate(migrator.add_column('pokestop', 'encounter_id', CharField(max_length=50, null=True)))

    if old_ver < 3:
        migrate(
            migrator.add_column('pokestop', 'active_fort_modifier', CharField(max_length=50, null=True)),
            migrator.drop_column('pokestop', 'encounter_id'),
            migrator.drop_column('pokestop', 'active_pokemon_id')
        )

    if old_ver < 4:
        db.drop_tables([ScannedLocation])

    if old_ver < 5:
        # Some pokemon were added before the 595 bug was "fixed".
        # Clean those up for a better UX.
        query = (Pokemon
                 .delete()
                 .where(Pokemon.disappear_time >
                        (datetime.utcnow() - timedelta(hours=24))))
        query.execute()

    if old_ver < 6:
        migrate(
            migrator.add_column('gym', 'last_scanned', DateTimeField(null=True)),
        )

    if old_ver < 7:
        migrate(
            migrator.drop_column('gymdetails', 'description'),
            migrator.add_column('gymdetails', 'description', TextField(null=True, default=""))
        )

    if old_ver < 8:
        migrate(
            migrator.add_column('pokemon', 'individual_attack', IntegerField(null=True, default=0)),
            migrator.add_column('pokemon', 'individual_defense', IntegerField(null=True, default=0)),
            migrator.add_column('pokemon', 'individual_stamina', IntegerField(null=True, default=0)),
            migrator.add_column('pokemon', 'move_1', IntegerField(null=True, default=0)),
            migrator.add_column('pokemon', 'move_2', IntegerField(null=True, default=0))
        )

    if old_ver < 9:
        migrate(
            migrator.add_column('pokemon', 'last_modified', DateTimeField(null=True, index=True)),
            migrator.add_column('pokestop', 'last_updated', DateTimeField(null=True, index=True))
        )

    if old_ver < 10:
        migrate(
            migrator.add_column('pokemon', 'time_detail', IntegerField(default=-1, index=True))
        )
