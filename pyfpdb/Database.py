#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Database.py

Create and manage the database objects.
"""
#    Copyright 2008-2011, Ray E. Barker
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

import L10n
_ = L10n.get_translation()

########################################################################

# TODO:  - rebuild indexes / vacuum option
#        - check speed of get_stats_from_hand() - add log info
#        - check size of db, seems big? (mysql)
#        - investigate size of mysql db (200K for just 7K hands? 2GB for 140K hands?)

# postmaster -D /var/lib/pgsql/data

#    Standard Library modules
import os
import sys
import traceback
from datetime import datetime, date, time, timedelta
from time import time, strftime, sleep
from decimal_wrapper import Decimal
import string
import re
import Queue
import codecs
import math 
import pytz
import logging

re_char = re.compile('[^a-zA-Z]')

#    FreePokerTools modules
import SQL
import Card
import Charset
from Exceptions import *
import Configuration

if __name__ == "__main__":
    Configuration.set_logfile("fpdb-log.txt")
# logging has been set up in fpdb.py or HUD_main.py, use their settings:
log = logging.getLogger("db")

#    Other library modules
try:
    import sqlalchemy.pool as pool
    use_pool = True
except ImportError:
    log.info(_("Not using sqlalchemy connection pool."))
    use_pool = False

try:
    from numpy import var
    use_numpy = True
except ImportError:
    log.info(_("Not using numpy to define variance in sqlite."))
    use_numpy = False


DB_VERSION = 190

# Variance created as sqlite has a bunch of undefined aggregate functions.

class VARIANCE:
    def __init__(self):
        self.store = []

    def step(self, value):
        self.store.append(value)

    def finalize(self):
        return float(var(self.store))

class sqlitemath:
    def mod(self, a, b):
        return a%b
    
    
# These are for appendStats. Insert new stats at the right place, because
# SQL needs strict order.
# Keys used to index into player data in storeHandsPlayers.
HANDS_PLAYERS_KEYS = [
    'startCash',
    'effStack',
    'seatNo',
    'sitout',
    'card1',
    'card2',
    'card3',
    'card4',
    'card5',
    'card6',
    'card7',
    'card8',
    'card9',
    'card10',
    'card11',
    'card12',
    'card13',
    'card14',
    'card15',
    'card16',
    'card17',
    'card18',
    'card19',
    'card20',
    'played',
    'winnings',
    'rake',
    'rakeDealt',
    'rakeContributed',
    'rakeWeighted',
    'showdownWinnings',
    'nonShowdownWinnings',
    'totalProfit',
    'allInEV',
    'BBwon',
    'vsHero',
    'street0VPIChance',
    'street0VPI',
    'street1Seen',
    'street2Seen',
    'street3Seen',
    'street4Seen',
    'sawShowdown',
    'showed',
    'wonAtSD',
    'street0AggrChance',
    'street0Aggr',
    'street1Aggr',
    'street2Aggr',
    'street3Aggr',
    'street4Aggr',
    'street1CBChance',
    'street2CBChance',
    'street3CBChance',
    'street4CBChance',
    'street1CBDone',
    'street2CBDone',
    'street3CBDone',
    'street4CBDone',
    'wonWhenSeenStreet1',
    'wonWhenSeenStreet2',
    'wonWhenSeenStreet3',
    'wonWhenSeenStreet4',
    'street0Calls',
    'street1Calls',
    'street2Calls',
    'street3Calls',
    'street4Calls',
    'street0Bets',
    'street1Bets',
    'street2Bets',
    'street3Bets',
    'street4Bets',
    'position',
    'tourneysPlayersIds',
    'startCards',
    'street0CalledRaiseChance',
    'street0CalledRaiseDone',
    'street0_3BChance',
    'street0_3BDone',
    'street0_4BChance',
    'street0_4BDone',
    'street0_C4BChance',
    'street0_C4BDone',
    'street0_FoldTo3BChance',
    'street0_FoldTo3BDone',
    'street0_FoldTo4BChance',
    'street0_FoldTo4BDone',
    'street0_SqueezeChance',
    'street0_SqueezeDone',
    'raiseToStealChance',
    'raiseToStealDone',
    'success_Steal',
    'otherRaisedStreet0',
    'otherRaisedStreet1',
    'otherRaisedStreet2',
    'otherRaisedStreet3',
    'otherRaisedStreet4',
    'foldToOtherRaisedStreet0',
    'foldToOtherRaisedStreet1',
    'foldToOtherRaisedStreet2',
    'foldToOtherRaisedStreet3',
    'foldToOtherRaisedStreet4',
    'raiseFirstInChance',
    'raisedFirstIn',
    'foldBbToStealChance',
    'foldedBbToSteal',
    'foldSbToStealChance',
    'foldedSbToSteal',
    'foldToStreet1CBChance',
    'foldToStreet1CBDone',
    'foldToStreet2CBChance',
    'foldToStreet2CBDone',
    'foldToStreet3CBChance',
    'foldToStreet3CBDone',
    'foldToStreet4CBChance',
    'foldToStreet4CBDone',
    'street1CheckCallRaiseChance',
    'street1CheckCallDone',
    'street1CheckRaiseDone',
    'street2CheckCallRaiseChance',
    'street2CheckCallDone',
    'street2CheckRaiseDone',
    'street3CheckCallRaiseChance',
    'street3CheckCallDone',
    'street3CheckRaiseDone',
    'street4CheckCallRaiseChance',
    'street4CheckCallDone',
    'street4CheckRaiseDone',
    'street0Raises',
    'street1Raises',
    'street2Raises',
    'street3Raises',
    'street4Raises',
    'handString'
]

# Just like STATS_KEYS, this lets us efficiently add data at the
# "beginning" later.
HANDS_PLAYERS_KEYS.reverse()


CACHE_KEYS = [
    'hands',
    'played',
    'street0VPIChance',
    'street0VPI',
    'street0AggrChance',
    'street0Aggr',
    'street0CalledRaiseChance',
    'street0CalledRaiseDone',
    'street0_3BChance',
    'street0_3BDone',
    'street0_4BChance',
    'street0_4BDone',
    'street0_C4BChance',
    'street0_C4BDone',
    'street0_FoldTo3BChance',
    'street0_FoldTo3BDone',
    'street0_FoldTo4BChance',
    'street0_FoldTo4BDone',
    'street0_SqueezeChance',
    'street0_SqueezeDone',
    'raiseToStealChance',
    'raiseToStealDone',
    'success_Steal',
    'street1Seen',
    'street2Seen',
    'street3Seen',
    'street4Seen',
    'sawShowdown',
    'street1Aggr',
    'street2Aggr',
    'street3Aggr',
    'street4Aggr',
    'otherRaisedStreet0',
    'otherRaisedStreet1',
    'otherRaisedStreet2',
    'otherRaisedStreet3',
    'otherRaisedStreet4',
    'foldToOtherRaisedStreet0',
    'foldToOtherRaisedStreet1',
    'foldToOtherRaisedStreet2',
    'foldToOtherRaisedStreet3',
    'foldToOtherRaisedStreet4',
    'wonWhenSeenStreet1',
    'wonWhenSeenStreet2',
    'wonWhenSeenStreet3',
    'wonWhenSeenStreet4',
    'wonAtSD',
    'raiseFirstInChance',
    'raisedFirstIn',
    'foldBbToStealChance',
    'foldedBbToSteal',
    'foldSbToStealChance',
    'foldedSbToSteal',
    'street1CBChance',
    'street1CBDone',
    'street2CBChance',
    'street2CBDone',
    'street3CBChance',
    'street3CBDone',
    'street4CBChance',
    'street4CBDone',
    'foldToStreet1CBChance',
    'foldToStreet1CBDone',
    'foldToStreet2CBChance',
    'foldToStreet2CBDone',
    'foldToStreet3CBChance',
    'foldToStreet3CBDone',
    'foldToStreet4CBChance',
    'foldToStreet4CBDone',
    'totalProfit',
    'rake',
    'rakeDealt',
    'rakeContributed',
    'rakeWeighted',
    'showdownWinnings',
    'nonShowdownWinnings',
    'allInEV',
    'BBwon',
    'vsHero',
    'street1CheckCallRaiseChance',
    'street1CheckCallDone',
    'street1CheckRaiseDone',
    'street2CheckCallRaiseChance',
    'street2CheckCallDone',
    'street2CheckRaiseDone',
    'street3CheckCallRaiseChance',
    'street3CheckCallDone',
    'street3CheckRaiseDone',
    'street4CheckCallRaiseChance',
    'street4CheckCallDone',
    'street4CheckRaiseDone',
    'street0Calls',
    'street1Calls',
    'street2Calls',
    'street3Calls',
    'street4Calls',
    'street0Bets',
    'street1Bets',
    'street2Bets',
    'street3Bets',
    'street4Bets',
    'street0Raises',
    'street1Raises',
    'street2Raises',
    'street3Raises',
    'street4Raises',
    ]


class Database:

    MYSQL_INNODB = 2
    PGSQL = 3
    SQLITE = 4

    hero_hudstart_def = '1999-12-31'      # default for length of Hero's stats in HUD
    villain_hudstart_def = '1999-12-31'   # default for length of Villain's stats in HUD

    # Data Structures for index and foreign key creation
    # drop_code is an int with possible values:  0 - don't drop for bulk import
    #                                            1 - drop during bulk import
    # db differences:
    # - note that mysql automatically creates indexes on constrained columns when
    #   foreign keys are created, while postgres does not. Hence the much longer list
    #   of indexes is required for postgres.
    # all primary keys are left on all the time
    #
    #             table     column           drop_code

    indexes = [
                [ ] # no db with index 0
              , [ ] # no db with index 1
              , [ # indexes for mysql (list index 2) (foreign keys not here, in next data structure)
                #  {'tab':'Players',         'col':'name',              'drop':0}  unique indexes not dropped
                #  {'tab':'Hands',           'col':'siteHandNo',        'drop':0}  unique indexes not dropped
                #, {'tab':'Tourneys',        'col':'siteTourneyNo',     'drop':0}  unique indexes not dropped
                ]
              , [ # indexes for postgres (list index 3)
                  {'tab':'Gametypes',       'col':'siteId',            'drop':0}
                , {'tab':'Hands',           'col':'tourneyId',         'drop':0} # mct 22/3/09
                , {'tab':'Hands',           'col':'gametypeId',        'drop':0} # mct 22/3/09
                , {'tab':'Hands',           'col':'sessionId',         'drop':0} # mct 22/3/09
                , {'tab':'Hands',           'col':'fileId',            'drop':0} # mct 22/3/09
                #, {'tab':'Hands',           'col':'siteHandNo',        'drop':0}  unique indexes not dropped
                , {'tab':'HandsActions',    'col':'handId',            'drop':1}
                , {'tab':'HandsActions',    'col':'playerId',          'drop':1}
                , {'tab':'HandsActions',    'col':'actionId',          'drop':1}
                , {'tab':'HandsStove',      'col':'handId',            'drop':1}
                , {'tab':'HandsStove',      'col':'playerId',          'drop':1}
                , {'tab':'HandsStove',      'col':'hiLo',              'drop':1}
                , {'tab':'HandsPots',       'col':'handId',            'drop':1}
                , {'tab':'HandsPots',       'col':'playerId',          'drop':1}
                , {'tab':'Boards',          'col':'handId',            'drop':1}
                , {'tab':'HandsPlayers',    'col':'handId',            'drop':1}
                , {'tab':'HandsPlayers',    'col':'playerId',          'drop':1}
                , {'tab':'HandsPlayers',    'col':'tourneysPlayersId', 'drop':0}
                , {'tab':'HandsPlayers',    'col':'startCards',        'drop':1}
                , {'tab':'HudCache',        'col':'gametypeId',        'drop':1}
                , {'tab':'HudCache',        'col':'playerId',          'drop':0}
                , {'tab':'HudCache',        'col':'tourneyTypeId',     'drop':0}
                , {'tab':'SessionsCache',   'col':'weekId',            'drop':1}
                , {'tab':'SessionsCache',   'col':'monthId',           'drop':1}
                , {'tab':'CashCache',       'col':'sessionId',         'drop':1}
                , {'tab':'CashCache',       'col':'gametypeId',        'drop':1}
                , {'tab':'CashCache',       'col':'playerId',          'drop':0}
                , {'tab':'TourCache',       'col':'sessionId',         'drop':1}
                , {'tab':'TourCache',       'col':'tourneyId',         'drop':1}
                , {'tab':'TourCache',       'col':'playerId',          'drop':0}
                , {'tab':'Players',         'col':'siteId',            'drop':1}
                #, {'tab':'Players',         'col':'name',              'drop':0}  unique indexes not dropped
                , {'tab':'Tourneys',        'col':'tourneyTypeId',     'drop':1}
                , {'tab':'Tourneys',        'col':'sessionId',         'drop':1}
                #, {'tab':'Tourneys',        'col':'siteTourneyNo',     'drop':0}  unique indexes not dropped
                , {'tab':'TourneysPlayers', 'col':'playerId',          'drop':0}
                #, {'tab':'TourneysPlayers', 'col':'tourneyId',         'drop':0}  unique indexes not dropped
                , {'tab':'TourneyTypes',    'col':'siteId',            'drop':0}
                , {'tab':'Backings',        'col':'tourneysPlayersId', 'drop':0}
                , {'tab':'Backings',        'col':'playerId',          'drop':0}
                , {'tab':'RawHands',        'col':'id',                'drop':0}
                , {'tab':'RawTourneys',     'col':'id',                'drop':0}
                ]
              , [ # indexes for sqlite (list index 4)
                  {'tab':'Hands',           'col':'tourneyId',         'drop':0}
                , {'tab':'Hands',           'col':'gametypeId',        'drop':0}
                , {'tab':'Hands',           'col':'sessionId',         'drop':0}
                , {'tab':'Hands',           'col':'fileId',            'drop':0}
                , {'tab':'Boards',          'col':'handId',            'drop':0}
                , {'tab':'Gametypes',       'col':'siteId',            'drop':0}
                , {'tab':'HandsPlayers',    'col':'handId',            'drop':0}
                , {'tab':'HandsPlayers',    'col':'playerId',          'drop':0}
                , {'tab':'HandsPlayers',    'col':'tourneysPlayersId', 'drop':0}
                , {'tab':'HandsActions',    'col':'handId',            'drop':0}
                , {'tab':'HandsActions',    'col':'playerId',          'drop':0}
                , {'tab':'HandsActions',    'col':'actionId',          'drop':1}
                , {'tab':'HandsStove',      'col':'handId',            'drop':0}
                , {'tab':'HandsStove',      'col':'playerId',          'drop':0}
                , {'tab':'HandsPots',       'col':'handId',            'drop':0}
                , {'tab':'HandsPots',       'col':'playerId',          'drop':0}
                , {'tab':'HudCache',        'col':'gametypeId',        'drop':1}
                , {'tab':'HudCache',        'col':'playerId',          'drop':0}
                , {'tab':'HudCache',        'col':'tourneyTypeId',     'drop':0}
                , {'tab':'SessionsCache',   'col':'weekId',            'drop':1}
                , {'tab':'SessionsCache',   'col':'monthId',           'drop':1}
                , {'tab':'CashCache',       'col':'sessionId',         'drop':1}
                , {'tab':'CashCache',       'col':'gametypeId',        'drop':1}
                , {'tab':'CashCache',       'col':'playerId',          'drop':0}
                , {'tab':'TourCache',       'col':'sessionId',         'drop':1}
                , {'tab':'TourCache',       'col':'tourneyId',         'drop':1}
                , {'tab':'TourCache',       'col':'playerId',          'drop':0}
                , {'tab':'Players',         'col':'siteId',            'drop':1}
                , {'tab':'Tourneys',        'col':'tourneyTypeId',     'drop':1}
                , {'tab':'Tourneys',        'col':'sessionId',         'drop':1}
                , {'tab':'TourneysPlayers', 'col':'playerId',          'drop':0}
                , {'tab':'TourneyTypes',    'col':'siteId',            'drop':0}
                , {'tab':'Backings',        'col':'tourneysPlayersId', 'drop':0}
                , {'tab':'Backings',        'col':'playerId',          'drop':0}
                , {'tab':'RawHands',        'col':'id',                'drop':0}
                , {'tab':'RawTourneys',     'col':'id',                'drop':0}
                ]
              ]
              
    
    foreignKeys = [
                    [ ] # no db with index 0
                  , [ ] # no db with index 1
                  , [ # foreign keys for mysql (index 2)
                      {'fktab':'Hands',        'fkcol':'tourneyId',     'rtab':'Tourneys',      'rcol':'id', 'drop':1}
                    , {'fktab':'Hands',        'fkcol':'gametypeId',    'rtab':'Gametypes',     'rcol':'id', 'drop':1}
                    , {'fktab':'Hands',        'fkcol':'sessionId',     'rtab':'SessionsCache', 'rcol':'id', 'drop':1}
                    , {'fktab':'Hands',        'fkcol':'fileId',        'rtab':'Files',         'rcol':'id', 'drop':1}
                    , {'fktab':'Boards',       'fkcol':'handId',        'rtab':'Hands',         'rcol':'id', 'drop':1}
                    , {'fktab':'HandsPlayers', 'fkcol':'handId',        'rtab':'Hands',         'rcol':'id', 'drop':1}
                    , {'fktab':'HandsPlayers', 'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':1}
                    , {'fktab':'HandsPlayers', 'fkcol':'tourneysPlayersId','rtab':'TourneysPlayers','rcol':'id', 'drop':1}
                    , {'fktab':'HandsPlayers', 'fkcol':'startCards',    'rtab':'StartCards',    'rcol':'id', 'drop':1}
                    , {'fktab':'HandsActions', 'fkcol':'handId',        'rtab':'Hands',         'rcol':'id', 'drop':1}
                    , {'fktab':'HandsActions', 'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':1}
                    , {'fktab':'HandsActions', 'fkcol':'actionId',      'rtab':'Actions',       'rcol':'id', 'drop':1}
                    , {'fktab':'HandsStove',   'fkcol':'handId',        'rtab':'Hands',         'rcol':'id', 'drop':1}
                    , {'fktab':'HandsStove',   'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':1}
                    , {'fktab':'HandsStove',   'fkcol':'rankId',        'rtab':'Rank',          'rcol':'id', 'drop':1}
                    , {'fktab':'HandsPots',    'fkcol':'handId',        'rtab':'Hands',         'rcol':'id', 'drop':1}
                    , {'fktab':'HandsPots',    'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':1}
                    , {'fktab':'HudCache',     'fkcol':'gametypeId',    'rtab':'Gametypes',     'rcol':'id', 'drop':1}
                    , {'fktab':'HudCache',     'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':0}
                    , {'fktab':'HudCache',     'fkcol':'tourneyTypeId', 'rtab':'TourneyTypes',  'rcol':'id', 'drop':1}
                    , {'fktab':'SessionsCache','fkcol':'weekId',        'rtab':'WeeksCache',    'rcol':'id', 'drop':1}
                    , {'fktab':'SessionsCache','fkcol':'monthId',       'rtab':'MonthsCache',   'rcol':'id', 'drop':1}
                    , {'fktab':'CashCache',    'fkcol':'sessionId',     'rtab':'SessionsCache', 'rcol':'id', 'drop':1}
                    , {'fktab':'CashCache',    'fkcol':'gametypeId',    'rtab':'Gametypes',     'rcol':'id', 'drop':1}
                    , {'fktab':'CashCache',    'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':0}
                    , {'fktab':'TourCache',    'fkcol':'sessionId',     'rtab':'SessionsCache', 'rcol':'id', 'drop':1}
                    , {'fktab':'TourCache',    'fkcol':'tourneyId',     'rtab':'Tourneys',      'rcol':'id', 'drop':1}
                    , {'fktab':'TourCache',    'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':0}
                    , {'fktab':'Tourneys',     'fkcol':'tourneyTypeId', 'rtab':'TourneyTypes',  'rcol':'id', 'drop':1}
                    , {'fktab':'Tourneys',     'fkcol':'sessionId',     'rtab':'SessionsCache', 'rcol':'id', 'drop':1}
                    ]
                  , [ # foreign keys for postgres (index 3)
                      {'fktab':'Hands',        'fkcol':'tourneyId',     'rtab':'Tourneys',      'rcol':'id', 'drop':1}
                    , {'fktab':'Hands',        'fkcol':'gametypeId',    'rtab':'Gametypes',     'rcol':'id', 'drop':1}
                    , {'fktab':'Hands',        'fkcol':'sessionId',     'rtab':'SessionsCache', 'rcol':'id', 'drop':1}
                    , {'fktab':'Hands',        'fkcol':'fileId',        'rtab':'Files',         'rcol':'id', 'drop':1}
                    , {'fktab':'Boards',       'fkcol':'handId',        'rtab':'Hands',         'rcol':'id', 'drop':1}
                    , {'fktab':'HandsPlayers', 'fkcol':'handId',        'rtab':'Hands',         'rcol':'id', 'drop':1}
                    , {'fktab':'HandsPlayers', 'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':1}
                    , {'fktab':'HandsPlayers', 'fkcol':'tourneysPlayersId','rtab':'TourneysPlayers','rcol':'id', 'drop':1}
                    , {'fktab':'HandsPlayers', 'fkcol':'startCards',    'rtab':'StartCards',    'rcol':'id', 'drop':1}
                    , {'fktab':'HandsActions', 'fkcol':'handId',        'rtab':'Hands',         'rcol':'id', 'drop':1}
                    , {'fktab':'HandsActions', 'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':1}
                    , {'fktab':'HandsActions', 'fkcol':'actionId',      'rtab':'Actions',       'rcol':'id', 'drop':1}
                    , {'fktab':'HandsStove',   'fkcol':'handId',        'rtab':'Hands',         'rcol':'id', 'drop':1}
                    , {'fktab':'HandsStove',   'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':1}
                    , {'fktab':'HandsStove',   'fkcol':'rankId',        'rtab':'Rank',          'rcol':'id', 'drop':1}
                    , {'fktab':'HandsPots',    'fkcol':'handId',        'rtab':'Hands',         'rcol':'id', 'drop':1}
                    , {'fktab':'HandsPots',    'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':1}
                    , {'fktab':'HudCache',     'fkcol':'gametypeId',    'rtab':'Gametypes',     'rcol':'id', 'drop':1}
                    , {'fktab':'HudCache',     'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':0}
                    , {'fktab':'HudCache',     'fkcol':'tourneyTypeId', 'rtab':'TourneyTypes',  'rcol':'id', 'drop':1}
                    , {'fktab':'SessionsCache','fkcol':'weekId',        'rtab':'WeeksCache',    'rcol':'id', 'drop':1}
                    , {'fktab':'SessionsCache','fkcol':'monthId',       'rtab':'MonthsCache',   'rcol':'id', 'drop':1}
                    , {'fktab':'CashCache',    'fkcol':'sessionId',     'rtab':'SessionsCache', 'rcol':'id', 'drop':1}
                    , {'fktab':'CashCache',    'fkcol':'gametypeId',    'rtab':'Gametypes',     'rcol':'id', 'drop':1}
                    , {'fktab':'CashCache',    'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':0}
                    , {'fktab':'TourCache',    'fkcol':'sessionId',     'rtab':'SessionsCache', 'rcol':'id', 'drop':1}
                    , {'fktab':'TourCache',    'fkcol':'tourneyId',     'rtab':'Tourneys',      'rcol':'id', 'drop':1}
                    , {'fktab':'TourCache',    'fkcol':'playerId',      'rtab':'Players',       'rcol':'id', 'drop':0}
                    , {'fktab':'Tourneys',     'fkcol':'tourneyTypeId', 'rtab':'TourneyTypes',  'rcol':'id', 'drop':1}
                    , {'fktab':'Tourneys',     'fkcol':'sessionId',     'rtab':'SessionsCache', 'rcol':'id', 'drop':1}
                    ]
                  , [ # no foreign keys in sqlite (index 4)
                    ]
                  ]


    # MySQL Notes:
    #    "FOREIGN KEY (handId) REFERENCES Hands(id)" - requires index on Hands.id
    #                                                - creates index handId on <thistable>.handId
    # alter table t drop foreign key fk
    # alter table t add foreign key (fkcol) references tab(rcol)
    # alter table t add constraint c foreign key (fkcol) references tab(rcol)
    # (fkcol is used for foreigh key name)

    # mysql to list indexes: (CG - "LIST INDEXES" should work too)
    #   SELECT table_name, index_name, non_unique, column_name
    #   FROM INFORMATION_SCHEMA.STATISTICS
    #     WHERE table_name = 'tbl_name'
    #     AND table_schema = 'db_name'
    #   ORDER BY table_name, index_name, seq_in_index
    #
    # ALTER TABLE Tourneys ADD INDEX siteTourneyNo(siteTourneyNo)
    # ALTER TABLE tab DROP INDEX idx

    # mysql to list fks:
    #   SELECT constraint_name, table_name, column_name, referenced_table_name, referenced_column_name
    #   FROM information_schema.KEY_COLUMN_USAGE
    #   WHERE REFERENCED_TABLE_SCHEMA = (your schema name here)
    #   AND REFERENCED_TABLE_NAME is not null
    #   ORDER BY TABLE_NAME, COLUMN_NAME;

    # this may indicate missing object
    # _mysql_exceptions.OperationalError: (1025, "Error on rename of '.\\fpdb\\hands' to '.\\fpdb\\#sql2-7f0-1b' (errno: 152)")


    # PG notes:

    #  To add a foreign key constraint to a table:
    #  ALTER TABLE tab ADD CONSTRAINT c FOREIGN KEY (col) REFERENCES t2(col2) MATCH FULL;
    #  ALTER TABLE tab DROP CONSTRAINT zipchk
    #
    #  Note: index names must be unique across a schema
    #  CREATE INDEX idx ON tab(col)
    #  DROP INDEX idx
    #  SELECT * FROM PG_INDEXES

    # SQLite notes:

    # To add an index:
    # create index indexname on tablename (col);


    def __init__(self, c, sql = None, autoconnect = True):
        self.config = c
        self.__connected = False
        self.settings = {}
        self.settings['os'] = "linuxmac" if os.name != "nt" else "windows"
        db_params = c.get_db_parameters()
        self.import_options = c.get_import_parameters()
        self.backend = db_params['db-backend']
        self.db_server = db_params['db-server']
        self.database = db_params['db-databaseName']
        self.host = db_params['db-host']
        self.db_path = ''
        gen = c.get_general_params()
        self.day_start = 0
        self._hero = None
        self._has_lock = False
        self.printdata = False
        self.resetCache()
        self.resetBulkCache()
        
        if 'day_start' in gen:
            self.day_start = float(gen['day_start'])
            
        self.sessionTimeout = float(self.import_options['sessionTimeout'])
        self.publicDB = self.import_options['publicDB']

        # where possible avoid creating new SQL instance by using the global one passed in
        if sql is None:
            self.sql = SQL.Sql(db_server = self.db_server)
        else:
            self.sql = sql

        if autoconnect:
            # connect to db
            self.do_connect(c)

            if self.backend == self.PGSQL:
                from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_SERIALIZABLE
                #ISOLATION_LEVEL_AUTOCOMMIT     = 0
                #ISOLATION_LEVEL_READ_COMMITTED = 1
                #ISOLATION_LEVEL_SERIALIZABLE   = 2


            if self.backend == self.SQLITE and self.database == ':memory:' and self.wrongDbVersion and self.is_connected():
                log.info("sqlite/:memory: - creating")
                self.recreate_tables()
                self.wrongDbVersion = False
            
            self.gtcache    = None       # GameTypeId cache 
            self.tcache     = None       # TourneyId cache
            self.pcache     = None       # PlayerId cache
            self.tpcache    = None       # TourneysPlayersId cache

            # if fastStoreHudCache is true then the hudcache will be build using the limited configuration which ignores date, seats, and position
            self.build_full_hudcache = not self.import_options['fastStoreHudCache']
            self.cacheSessions = self.import_options['cacheSessions']
            self.callHud = self.import_options['callFpdbHud']

            #self.hud_hero_style = 'T'  # Duplicate set of vars just for hero - not used yet.
            #self.hud_hero_hands = 2000 # Idea is that you might want all-time stats for others
            #self.hud_hero_days  = 30   # but last T days or last H hands for yourself

            # vars for hand ids or dates fetched according to above config:
            self.hand_1day_ago = 0             # max hand id more than 24 hrs earlier than now
            self.date_ndays_ago = 'd000000'    # date N days ago ('d' + YYMMDD)
            self.h_date_ndays_ago = 'd000000'  # date N days ago ('d' + YYMMDD) for hero
            self.date_nhands_ago = {}          # dates N hands ago per player - not used yet

            self.saveActions = False if self.import_options['saveActions'] == False else True

            if self.is_connected():
                if not self.wrongDbVersion:
                    self.get_sites()
                self.connection.rollback()  # make sure any locks taken so far are released
    #end def __init__

    def dumpDatabase(self):
        result="fpdb database dump\nDB version=" + str(DB_VERSION)+"\n\n"

        tables=self.cursor.execute(self.sql.query['list_tables'])
        tables=self.cursor.fetchall()
        for table in (u'Actions', u'Autorates', u'Backings', u'Gametypes', u'Hands', u'Boards', u'HandsActions', u'HandsPlayers', u'HandsStove', u'Files', u'HudCache', u'SessionsCache', u'CashCache', u'TourCache',u'Players', u'RawHands', u'RawTourneys', u'Settings', u'Sites', u'TourneyTypes', u'Tourneys', u'TourneysPlayers'):
            print "table:", table
            result+="###################\nTable "+table+"\n###################\n"
            rows=self.cursor.execute(self.sql.query['get'+table])
            rows=self.cursor.fetchall()
            columnNames=self.cursor.description
            if not rows:
                result+="empty table\n"
            else:
                for row in rows:
                    for columnNumber in range(len(columnNames)):
                        if columnNames[columnNumber][0]=="importTime":
                            result+=("  "+columnNames[columnNumber][0]+"=ignore\n")
                        elif columnNames[columnNumber][0]=="styleKey":
                            result+=("  "+columnNames[columnNumber][0]+"=ignore\n")
                        else:
                            result+=("  "+columnNames[columnNumber][0]+"="+str(row[columnNumber])+"\n")
                    result+="\n"
            result+="\n"
        return result
    #end def dumpDatabase

    # could be used by hud to change hud style
    def set_hud_style(self, style):
        self.hud_style = style

    def do_connect(self, c):
        if c is None:
            raise FpdbError('Configuration not defined')

        db = c.get_db_parameters()
        try:
            self.connect(backend=db['db-backend'],
                         host=db['db-host'],
                         port=db['db-port'],
                         database=db['db-databaseName'],
                         user=db['db-user'],
                         password=db['db-password'])
        except:
            # error during connect
            self.__connected = False
            raise

        db_params = c.get_db_parameters()
        self.import_options = c.get_import_parameters()
        self.backend = db_params['db-backend']
        self.db_server = db_params['db-server']
        self.database = db_params['db-databaseName']
        self.host = db_params['db-host']
        self.port = db_params['db-port']

    def connect(self, backend=None, host=None, port=None, database=None,
                user=None, password=None, create=False):
        """Connects a database with the given parameters"""
        if backend is None:
            raise FpdbError('Database backend not defined')
        self.backend = backend
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor     = None
        self.hand_inc   = 1

        import psycopg2
        import psycopg2.extensions
        if use_pool:
            psycopg2 = pool.manage(psycopg2, pool_size=5)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        # If DB connection is made over TCP, then the variables
        # host, user and password are required
        # For local domain-socket connections, only DB name is
        # needed, and everything else is in fact undefined and/or
        # flat out wrong
        # sqlcoder: This database only connect failed in my windows setup??
        # Modifed it to try the 4 parameter style if the first connect fails - does this work everywhere?
        self.__connected = False
        if self.host == "localhost" or self.host == "127.0.0.1":
            try:
                self.connection = psycopg2.connect(database = database)
                self.__connected = True
            except:
                # direct connection failed so try user/pass/... version
                pass
        if not self.is_connected():
            try:
                #print(host, user, password, database)
                self.connection = psycopg2.connect(
                                           host = host,
                                           port = port,
                                           user = user,
                                           password = password,
                                           database = database)
                self.__connected = True
            except Exception, ex:
                if 'Connection refused' in ex.args[0] or ('database "' in ex.args[0] and '" does not exist' in ex.args[0]):
                    # meaning eg. db not running
                    raise FpdbPostgresqlNoDatabase(errmsg = ex.args[0])
                elif 'password authentication' in ex.args[0]:
                    raise FpdbPostgresqlAccessDenied(errmsg = ex.args[0])
                elif 'role "' in ex.args[0] and '" does not exist' in ex.args[0]: #role "fpdb" does not exist
                    raise FpdbPostgresqlAccessDenied(errmsg = ex.args[0])
                else:
                    msg = ex.args[0]
                print msg
                raise FpdbError(msg)
        else:
            raise FpdbError("unrecognised database backend:"+str(backend))

        if self.is_connected():
            self.cursor = self.connection.cursor()
            self.cursor.execute(self.sql.query['set tx level'])
            self.check_version(database=database, create=create)

    def get_sites(self):
        self.cursor.execute("SELECT name,id FROM Sites")
        sites = self.cursor.fetchall()
        self.config.set_site_ids(sites)

    def check_version(self, database, create):
        self.wrongDbVersion = False
        try:
            self.cursor.execute("SELECT * FROM Settings")
            settings = self.cursor.fetchone()
            if settings[0] != DB_VERSION:
                log.error((_("Outdated or too new database version (%s).") % (settings[0])) + " " + _("Please recreate tables."))
                self.wrongDbVersion = True
        except:# _mysql_exceptions.ProgrammingError:
            if database !=  ":memory:":
                if create:
                    #print (_("Failed to read settings table.") + " - " + _("Recreating tables."))
                    log.info(_("Failed to read settings table.") + " - " + _("Recreating tables."))
                    self.recreate_tables()
                    self.check_version(database=database, create=False)
                else:
                    #print (_("Failed to read settings table.") + " - " + _("Please recreate tables."))
                    log.info(_("Failed to read settings table.") + " - " + _("Please recreate tables."))
                    self.wrongDbVersion = True
            else:
                self.wrongDbVersion = True
    #end def connect

    def commit(self):
        if self.backend != self.SQLITE:
            self.connection.commit()
        else:
            # sqlite commits can fail because of shared locks on the database (SQLITE_BUSY)
            # re-try commit if it fails in case this happened
            maxtimes = 5
            pause = 1
            ok = False
            for i in xrange(maxtimes):
                try:
                    ret = self.connection.commit()
                    #log.debug(_("commit finished ok, i = ")+str(i))
                    ok = True
                except:
                    log.debug(_("commit %s failed: info=%s value=%s") % (str(i), str(sys.exc_info()), str(sys.exc_value)))
                    sleep(pause)
                if ok: break
            if not ok:
                log.debug(_("commit failed"))
                raise FpdbError('sqlite commit failed')

    def rollback(self):
        self.connection.rollback()

    def connected(self):
        """ now deprecated, use is_connected() instead """
        return self.__connected

    def is_connected(self):
        return self.__connected

    def get_cursor(self, connect=False):
        if self.backend == Database.MYSQL_INNODB and os.name == 'nt':
            self.connection.ping(True)
        return self.connection.cursor()

    def close_connection(self):
        self.connection.close()

    def disconnect(self, due_to_error=False):
        """Disconnects the DB (rolls back if param is true, otherwise commits"""
        if due_to_error:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.cursor.close()
        self.connection.close()
        self.__connected = False

    def reconnect(self, due_to_error=False):
        """Reconnects the DB"""
        #print "started reconnect"
        self.disconnect(due_to_error)
        self.connect(self.backend, self.host, self.database, self.user, self.password)

    def get_backend_name(self):
        """Returns the name of the currently used backend"""
        if self.backend==2:
            return "MySQL InnoDB"
        elif self.backend==3:
            return "PostgreSQL"
        elif self.backend==4:
            return "SQLite"
        else:
            raise FpdbError("invalid backend")

    def get_db_info(self):
        return (self.host, self.database, self.user, self.password)

    def get_table_name(self, hand_id):
        c = self.connection.cursor()
        c.execute(self.sql.query['get_table_name'], (hand_id, ))
        row = c.fetchone()
        return row

    def get_table_info(self, hand_id):
        c = self.connection.cursor()
        c.execute(self.sql.query['get_table_name'], (hand_id, ))
        row = c.fetchone()
        l = list(row)
        if row[3] == "ring":   # cash game
            l.append(None)
            l.append(None)
            return l
        else:    # tournament
            tour_no, tab_no = re.split(" ", row[0], 1)
            l.append(tour_no)
            l.append(tab_no)
            return l

    def get_last_hand(self):
        c = self.connection.cursor()
        c.execute(self.sql.query['get_last_hand'])
        row = c.fetchone()
        return row[0]

    def get_xml(self, hand_id):
        c = self.connection.cursor()
        c.execute(self.sql.query['get_xml'], (hand_id))
        row = c.fetchone()
        return row[0]

    def get_recent_hands(self, last_hand):
        c = self.connection.cursor()
        c.execute(self.sql.query['get_recent_hands'], {'last_hand': last_hand})
        return c.fetchall()

    def get_gameinfo_from_hid(self, hand_id):
        # returns a gameinfo (gametype) dictionary suitable for passing
        #  to Hand.hand_factory
        c = self.connection.cursor()
        q = self.sql.query['get_gameinfo_from_hid']
        q = q.replace('%s', self.sql.query['placeholder'])
        c.execute (q, (hand_id, ))
        row = c.fetchone()
        gameinfo = {'sitename':row[0],'category':row[1],'base':row[2],'type':row[3],'limitType':row[4],
                'hilo':row[5],'sb':row[6],'bb':row[7], 'sbet':row[8],'bbet':row[9], 'currency':row[10], 'gametypeId':row[11]}
        return gameinfo
        
#   Query 'get_hand_info' does not exist, so it seems
#    def get_hand_info(self, new_hand_id):
#        c = self.connection.cursor()
#        c.execute(self.sql.query['get_hand_info'], new_hand_id)
#        return c.fetchall()      

    def getHandCount(self):
        c = self.connection.cursor()
        c.execute(self.sql.query['getHandCount'])
        return c.fetchone()[0]
    #end def getHandCount

    def getTourneyCount(self):
        c = self.connection.cursor()
        c.execute(self.sql.query['getTourneyCount'])
        return c.fetchone()[0]
    #end def getTourneyCount

    def getTourneyTypeCount(self):
        c = self.connection.cursor()
        c.execute(self.sql.query['getTourneyTypeCount'])
        return c.fetchone()[0]
    #end def getTourneyCount

    def getSiteTourneyNos(self, site):
        c = self.connection.cursor()
        q = self.sql.query['getSiteId']
        q = q.replace('%s', self.sql.query['placeholder'])
        c.execute(q, (site,))
        siteid = c.fetchone()[0]
        q = self.sql.query['getSiteTourneyNos']
        q = q.replace('%s', self.sql.query['placeholder'])
        c.execute(q, (siteid,))
        alist = []
        for row in c.fetchall():
            alist.append(row)
        return alist

    def get_actual_seat(self, hand_id, name):
        c = self.connection.cursor()
        c.execute(self.sql.query['get_actual_seat'], (hand_id, name))
        row = c.fetchone()
        return row[0]

    def get_cards(self, hand):
        """Get and return the cards for each player in the hand."""
        cards = {} # dict of cards, the key is the seat number,
                   # the value is a tuple of the players cards
                   # example: {1: (0, 0, 20, 21, 22, 0 , 0)}
        c = self.connection.cursor()
        c.execute(self.sql.query['get_cards'], [hand])
        for row in c.fetchall():
            cards[row[0]] = row[1:]
        return cards

    def get_common_cards(self, hand):
        """Get and return the community cards for the specified hand."""
        cards = {}
        c = self.connection.cursor()
        c.execute(self.sql.query['get_common_cards'], [hand])
#        row = c.fetchone()
        cards['common'] = c.fetchone()
        return cards

    def get_action_from_hand(self, hand_no):
        action = [ [], [], [], [], [] ]
        c = self.connection.cursor()
        c.execute(self.sql.query['get_action_from_hand'], (hand_no,))
        for row in c.fetchall():
            street = row[0]
            act = row[1:]
            action[street].append(act)
        return action

    def get_winners_from_hand(self, hand):
        """Returns a hash of winners:amount won, given a hand number."""
        winners = {}
        c = self.connection.cursor()
        c.execute(self.sql.query['get_winners_from_hand'], (hand,))
        for row in c.fetchall():
            winners[row[0]] = row[1]
        return winners

    def set_printdata(self, val):
        self.printdata = val

    def init_hud_stat_vars(self, hud_days, h_hud_days):
        """Initialise variables used by Hud to fetch stats:
           self.hand_1day_ago     handId of latest hand played more than a day ago
           self.date_ndays_ago    date n days ago
           self.h_date_ndays_ago  date n days ago for hero (different n)
        """
        self.hand_1day_ago = 1
        c = self.get_cursor()
        c.execute(self.sql.query['get_hand_1day_ago'])
        row = c.fetchone()
        if row and row[0]:
            self.hand_1day_ago = int(row[0])
                
        tz = datetime.utcnow() - datetime.today()
        tz_offset = tz.seconds/3600
        tz_day_start_offset = self.day_start + tz_offset
        
        d = timedelta(days=hud_days, hours=tz_day_start_offset)
        now = datetime.utcnow() - d
        self.date_ndays_ago = "d%02d%02d%02d" % (now.year - 2000, now.month, now.day)
        
        d = timedelta(days=h_hud_days, hours=tz_day_start_offset)
        now = datetime.utcnow() - d
        self.h_date_ndays_ago = "d%02d%02d%02d" % (now.year - 2000, now.month, now.day)

    # is get_stats_from_hand slow?
    # Gimick - yes  - reason being that the gametypeid join on hands
    # increases exec time on SQLite and postgres by a factor of 6 to 10
    # method below changed to lookup hand.gametypeid and pass that as
    # a constant to the underlying query.
    
    def get_stats_from_hand( self, hand, type   # type is "ring" or "tour"
                           , hud_params = {'hud_style':'A', 'agg_bb_mult':1000
                                          ,'seats_style':'A', 'seats_cust_nums_low':1, 'seats_cust_nums_high':10 
                                          ,'h_hud_style':'S', 'h_agg_bb_mult':1000
                                          ,'h_seats_style':'A', 'h_seats_cust_nums_low':1, 'h_seats_cust_nums_high':10 
                                          }
                           , hero_id = -1
                           , num_seats = 6
                           ):
        stat_range   = hud_params['stat_range']
        agg_bb_mult = hud_params['agg_bb_mult']
        seats_style = hud_params['seats_style']
        seats_cust_nums_low = hud_params['seats_cust_nums_low']
        seats_cust_nums_high = hud_params['seats_cust_nums_high']
        h_stat_range   = hud_params['h_stat_range']
        h_agg_bb_mult = hud_params['h_agg_bb_mult']
        h_seats_style = hud_params['h_seats_style']
        h_seats_cust_nums_low = hud_params['h_seats_cust_nums_low']
        h_seats_cust_nums_high = hud_params['h_seats_cust_nums_high']

        stat_dict = {}

        if seats_style == 'A':
            seats_min, seats_max = 0, 10
        elif seats_style == 'C':
            seats_min, seats_max = seats_cust_nums_low, seats_cust_nums_high
        elif seats_style == 'E':
            seats_min, seats_max = num_seats, num_seats
        else:
            seats_min, seats_max = 0, 10
            print "bad seats_style value:", seats_style

        if h_seats_style == 'A':
            h_seats_min, h_seats_max = 0, 10
        elif h_seats_style == 'C':
            h_seats_min, h_seats_max = h_seats_cust_nums_low, h_seats_cust_nums_high
        elif h_seats_style == 'E':
            h_seats_min, h_seats_max = num_seats, num_seats
        else:
            h_seats_min, h_seats_max = 0, 10
            print "bad h_seats_style value:", h_seats_style

        if stat_range == 'S' or h_stat_range == 'S':
            self.get_stats_from_hand_session(hand, stat_dict, hero_id
                                            ,stat_range, seats_min, seats_max
                                            ,h_stat_range, h_seats_min, h_seats_max)

            if stat_range == 'S' and h_stat_range == 'S':
                return stat_dict

        if stat_range == 'T':
            stylekey = self.date_ndays_ago
        elif stat_range == 'A':
            stylekey = '0000000'  # all stylekey values should be higher than this
        elif stat_range == 'S':
            stylekey = 'zzzzzzz'  # all stylekey values should be lower than this
        else:
            stylekey = '0000000'
            log.info('stat_range: %s' % stat_range)

        #elif stat_range == 'H':
        #    stylekey = date_nhands_ago  needs array by player here ...

        if h_stat_range == 'T':
            h_stylekey = self.h_date_ndays_ago
        elif h_stat_range == 'A':
            h_stylekey = '0000000'  # all stylekey values should be higher than this
        elif h_stat_range == 'S':
            h_stylekey = 'zzzzzzz'  # all stylekey values should be lower than this
        else:
            h_stylekey = '00000000'
            log.info('h_stat_range: %s' % h_stat_range)

        #elif h_stat_range == 'H':
        #    h_stylekey = date_nhands_ago  needs array by player here ...

        # lookup gametypeId from hand
        handinfo = self.get_gameinfo_from_hid(hand)
        gametypeId = handinfo["gametypeId"]

        query = 'get_stats_from_hand_aggregated'
        subs = (hand
               ,hero_id, stylekey, agg_bb_mult, agg_bb_mult, gametypeId, seats_min, seats_max  # hero params
               ,hero_id, h_stylekey, h_agg_bb_mult, h_agg_bb_mult, gametypeId, h_seats_min, h_seats_max)    # villain params

        stime = time()
        c = self.connection.cursor()

        # Now get the stats
        c.execute(self.sql.query[query], subs)
        ptime = time() - stime
        log.info("HudCache query get_stats_from_hand_aggregated took %.3f seconds" % ptime)
        colnames = [desc[0] for desc in c.description]
        for row in c.fetchall():
            playerid = row[0]
            if (playerid == hero_id and h_stat_range != 'S') or (playerid != hero_id and stat_range != 'S'):
                t_dict = {}
                for name, val in zip(colnames, row):
                    t_dict[name.lower()] = val
                stat_dict[t_dict['player_id']] = t_dict

        return stat_dict

    # uses query on handsplayers instead of hudcache to get stats on just this session
    def get_stats_from_hand_session(self, hand, stat_dict, hero_id
                                   ,stat_range, seats_min, seats_max
                                   ,h_stat_range, h_seats_min, h_seats_max):
        """Get stats for just this session (currently defined as any play in the last 24 hours - to
           be improved at some point ...)
           h_stat_range and stat_range params indicate whether to get stats for hero and/or others
           - only fetch heroes stats if h_stat_range == 'S',
             and only fetch others stats if stat_range == 'S'
           seats_min/max params give seats limits, only include stats if between these values
        """

        query = self.sql.query['get_stats_from_hand_session']
        if self.db_server == 'mysql':
            query = query.replace("<signed>", 'signed ')
        else:
            query = query.replace("<signed>", '')

        subs = (self.hand_1day_ago, hand, hero_id, seats_min, seats_max
                                        , hero_id, h_seats_min, h_seats_max)
        c = self.get_cursor()

        # now get the stats
        #print "sess_stats: subs =", subs, "subs[0] =", subs[0]
        c.execute(query, subs)
        colnames = [desc[0] for desc in c.description]
        n = 0

        row = c.fetchone()
        if colnames[0].lower() == 'player_id':

            # Loop through stats adding them to appropriate stat_dict:
            while row:
                playerid = row[0]
                seats = row[1]
                if (playerid == hero_id and h_stat_range == 'S') or (playerid != hero_id and stat_range == 'S'):
                    for name, val in zip(colnames, row):
                        if not playerid in stat_dict:
                            stat_dict[playerid] = {}
                            stat_dict[playerid][name.lower()] = val
                        elif not name.lower() in stat_dict[playerid]:
                            stat_dict[playerid][name.lower()] = val
                        elif name.lower() not in ('hand_id', 'player_id', 'seat', 'screen_name', 'seats'):
                            #print "DEBUG: stat_dict[%s][%s]: %s" %(playerid, name.lower(), val)
                            stat_dict[playerid][name.lower()] += val
                    n += 1
                    if n >= 10000: break  # todo: don't think this is needed so set nice and high
                                          # prevents infinite loop so leave for now - comment out or remove?
                row = c.fetchone()
        else:
            log.error(_("ERROR: query %s result does not have player_id as first column") % (query,))

        #print "   %d rows fetched, len(stat_dict) = %d" % (n, len(stat_dict))

        #print "session stat_dict =", stat_dict
        #return stat_dict

    def get_player_id(self, config, siteName, playerName):
        c = self.connection.cursor()
        siteNameUtf = Charset.to_utf8(siteName)
        playerNameUtf = unicode(playerName)
        #print "db.get_player_id siteName",siteName,"playerName",playerName
        c.execute(self.sql.query['get_player_id'], (playerNameUtf, siteNameUtf))
        row = c.fetchone()
        if row:
            return row[0]
        else:
            return None

    def get_player_names(self, config, site_id=None, like_player_name="%"):
        """Fetch player names from players. Use site_id and like_player_name if provided"""

        if site_id is None:
            site_id = -1
        c = self.get_cursor()
        p_name = Charset.to_utf8(like_player_name)
        c.execute(self.sql.query['get_player_names'], (p_name, site_id, site_id))
        rows = c.fetchall()
        return rows

    def get_site_id(self, site):
        c = self.get_cursor()
        c.execute(self.sql.query['getSiteId'], (site,))
        result = c.fetchall()
        return result

    def resetCache(self):
        self.ttold      = set()      # TourneyTypes old
        self.ttnew      = set()      # TourneyTypes new
        self.wmold      = set()      # WeeksMonths old
        self.wmnew      = set()      # WeeksMonths new
        self.gtcache    = None       # GameTypeId cache 
        self.tcache     = None       # TourneyId cache
        self.pcache     = None       # PlayerId cache
        self.tpcache    = None       # TourneysPlayersId cache

    def get_last_insert_id(self, cursor=None):
        ret = None
        try:
            if self.backend == self.MYSQL_INNODB:
                ret = self.connection.insert_id()
                if ret < 1 or ret > 999999999:
                    log.warning(_("getLastInsertId(): problem fetching insert_id? ret=%d") % ret)
                    ret = -1
            elif self.backend == self.PGSQL:
                # some options:
                # currval(hands_id_seq) - use name of implicit seq here
                # lastval() - still needs sequences set up?
                # insert ... returning  is useful syntax (but postgres specific?)
                # see rules (fancy trigger type things)
                c = self.get_cursor()
                ret = c.execute ("SELECT lastval()")
                row = c.fetchone()
                if not row:
                    log.warning(_("getLastInsertId(%s): problem fetching lastval? row=%d") % (seq, row))
                    ret = -1
                else:
                    ret = row[0]
            elif self.backend == self.SQLITE:
                ret = cursor.lastrowid
            else:
                log.error(_("getLastInsertId(): unknown backend: %d") % self.backend)
                ret = -1
        except:
            ret = -1
            err = traceback.extract_tb(sys.exc_info()[2])
            print _("*** Database get_last_insert_id error: ") + str(sys.exc_info()[1])
            print "\n".join( [e[0]+':'+str(e[1])+" "+e[2] for e in err] )
            raise
        return ret
    
    def prepareBulkImport(self):
        """Drop some indexes/foreign keys to prepare for bulk import.
           Currently keeping the standalone indexes as needed to import quickly"""
        stime = time()
        c = self.get_cursor()
        # sc: don't think autocommit=0 is needed, should already be in that mode
        if self.backend == self.MYSQL_INNODB:
            c.execute("SET foreign_key_checks=0")
            c.execute("SET autocommit=0")
            return
        if self.backend == self.PGSQL:
            self.connection.set_isolation_level(0)   # allow table/index operations to work
        for fk in self.foreignKeys[self.backend]:
            if fk['drop'] == 1:
                if self.backend == self.MYSQL_INNODB:
                    c.execute("SELECT constraint_name " +
                              "FROM information_schema.KEY_COLUMN_USAGE " +
                              #"WHERE REFERENCED_TABLE_SCHEMA = 'fpdb'
                              "WHERE 1=1 " +
                              "AND table_name = %s AND column_name = %s " +
                              "AND referenced_table_name = %s " +
                              "AND referenced_column_name = %s ",
                              (fk['fktab'], fk['fkcol'], fk['rtab'], fk['rcol']) )
                    cons = c.fetchone()
                    #print "preparebulk find fk: cons=", cons
                    if cons:
                        print "dropping mysql fk", cons[0], fk['fktab'], fk['fkcol']
                        try:
                            c.execute("alter table " + fk['fktab'] + " drop foreign key " + cons[0])
                        except:
                            print "    drop failed: " + str(sys.exc_info())
                elif self.backend == self.PGSQL:
    #    DON'T FORGET TO RECREATE THEM!!
                    print "dropping pg fk", fk['fktab'], fk['fkcol']
                    try:
                        # try to lock table to see if index drop will work:
                        # hmmm, tested by commenting out rollback in grapher. lock seems to work but
                        # then drop still hangs :-(  does work in some tests though??
                        # will leave code here for now pending further tests/enhancement ...
                        c.execute("BEGIN TRANSACTION")
                        c.execute( "lock table %s in exclusive mode nowait" % (fk['fktab'],) )
                        #print "after lock, status:", c.statusmessage
                        #print "alter table %s drop constraint %s_%s_fkey" % (fk['fktab'], fk['fktab'], fk['fkcol'])
                        try:
                            c.execute("alter table %s drop constraint %s_%s_fkey" % (fk['fktab'], fk['fktab'], fk['fkcol']))
                            print "dropped pg fk pg fk %s_%s_fkey, continuing ..." % (fk['fktab'], fk['fkcol'])
                        except:
                            if "does not exist" not in str(sys.exc_value):
                                print _("warning: drop pg fk %s_%s_fkey failed: %s, continuing ...") \
                                      % (fk['fktab'], fk['fkcol'], str(sys.exc_value).rstrip('\n') )
                        c.execute("END TRANSACTION")
                    except:
                        print _("warning: constraint %s_%s_fkey not dropped: %s, continuing ...") \
                              % (fk['fktab'],fk['fkcol'], str(sys.exc_value).rstrip('\n'))
                else:
                    return -1

        for idx in self.indexes[self.backend]:
            if idx['drop'] == 1:
                if self.backend == self.MYSQL_INNODB:
                    print _("dropping mysql index "), idx['tab'], idx['col']
                    try:
                        # apparently nowait is not implemented in mysql so this just hangs if there are locks
                        # preventing the index drop :-(
                        c.execute( "alter table %s drop index %s;", (idx['tab'],idx['col']) )
                    except:
                        print _("    drop index failed: ") + str(sys.exc_info())
                            # ALTER TABLE `fpdb`.`handsplayers` DROP INDEX `playerId`;
                            # using: 'HandsPlayers' drop index 'playerId'
                elif self.backend == self.PGSQL:
    #    DON'T FORGET TO RECREATE THEM!!
                    print _("dropping pg index "), idx['tab'], idx['col']
                    try:
                        # try to lock table to see if index drop will work:
                        c.execute("BEGIN TRANSACTION")
                        c.execute( "lock table %s in exclusive mode nowait" % (idx['tab'],) )
                        #print "after lock, status:", c.statusmessage
                        try:
                            # table locked ok so index drop should work:
                            #print "drop index %s_%s_idx" % (idx['tab'],idx['col'])
                            c.execute( "drop index if exists %s_%s_idx" % (idx['tab'],idx['col']) )
                            #print "dropped  pg index ", idx['tab'], idx['col']
                        except:
                            if "does not exist" not in str(sys.exc_value):
                                print _("warning: drop index %s_%s_idx failed: %s, continuing ...") \
                                      % (idx['tab'],idx['col'], str(sys.exc_value).rstrip('\n'))
                        c.execute("END TRANSACTION")
                    except:
                        print _("warning: index %s_%s_idx not dropped %s, continuing ...") \
                              % (idx['tab'],idx['col'], str(sys.exc_value).rstrip('\n'))
                else:
                    return -1

        if self.backend == self.PGSQL:
            self.connection.set_isolation_level(1)   # go back to normal isolation level
        self.commit() # seems to clear up errors if there were any in postgres
        ptime = time() - stime
        print (_("prepare import took %s seconds") % ptime)
    #end def prepareBulkImport

    def afterBulkImport(self):
        """Re-create any dropped indexes/foreign keys after bulk import"""
        stime = time()

        c = self.get_cursor()
        if self.backend == self.MYSQL_INNODB:
            c.execute("SET foreign_key_checks=1")
            c.execute("SET autocommit=1")
            return

        if self.backend == self.PGSQL:
            self.connection.set_isolation_level(0)   # allow table/index operations to work
        for fk in self.foreignKeys[self.backend]:
            if fk['drop'] == 1:
                if self.backend == self.MYSQL_INNODB:
                    c.execute("SELECT constraint_name " +
                              "FROM information_schema.KEY_COLUMN_USAGE " +
                              #"WHERE REFERENCED_TABLE_SCHEMA = 'fpdb'
                              "WHERE 1=1 " +
                              "AND table_name = %s AND column_name = %s " +
                              "AND referenced_table_name = %s " +
                              "AND referenced_column_name = %s ",
                              (fk['fktab'], fk['fkcol'], fk['rtab'], fk['rcol']) )
                    cons = c.fetchone()
                    #print "afterbulk: cons=", cons
                    if cons:
                        pass
                    else:
                        print _("Creating foreign key "), fk['fktab'], fk['fkcol'], "->", fk['rtab'], fk['rcol']
                        try:
                            c.execute("alter table " + fk['fktab'] + " add foreign key ("
                                      + fk['fkcol'] + ") references " + fk['rtab'] + "("
                                      + fk['rcol'] + ")")
                        except:
                            print _("Create foreign key failed: ") + str(sys.exc_info())
                elif self.backend == self.PGSQL:
                    print _("Creating foreign key "), fk['fktab'], fk['fkcol'], "->", fk['rtab'], fk['rcol']
                    try:
                        c.execute("alter table " + fk['fktab'] + " add constraint "
                                  + fk['fktab'] + '_' + fk['fkcol'] + '_fkey'
                                  + " foreign key (" + fk['fkcol']
                                  + ") references " + fk['rtab'] + "(" + fk['rcol'] + ")")
                    except:
                        print _("Create foreign key failed: ") + str(sys.exc_info())
                else:
                    return -1

        for idx in self.indexes[self.backend]:
            if idx['drop'] == 1:
                if self.backend == self.MYSQL_INNODB:
                    print _("Creating MySQL index %s %s") % (idx['tab'], idx['col'])
                    try:
                        s = "alter table %s add index %s(%s)" % (idx['tab'],idx['col'],idx['col'])
                        c.execute(s)
                    except:
                        print _("Create foreign key failed: ") + str(sys.exc_info())
                elif self.backend == self.PGSQL:
    #                pass
                    # mod to use tab_col for index name?
                    print _("Creating PostgreSQL index "), idx['tab'], idx['col']
                    try:
                        s = "create index %s_%s_idx on %s(%s)" % (idx['tab'], idx['col'], idx['tab'], idx['col'])
                        c.execute(s)
                    except:
                        print _("Create index failed: ") + str(sys.exc_info())
                else:
                    return -1

        if self.backend == self.PGSQL:
            self.connection.set_isolation_level(1)   # go back to normal isolation level
        self.commit()   # seems to clear up errors if there were any in postgres
        atime = time() - stime
        print (_("After import took %s seconds") % atime)
    #end def afterBulkImport

    def drop_referential_integrity(self):
        """Update all tables to remove foreign keys"""

        c = self.get_cursor()
        c.execute(self.sql.query['list_tables'])
        result = c.fetchall()

        for i in range(len(result)):
            c.execute("SHOW CREATE TABLE " + result[i][0])
            inner = c.fetchall()

            for j in range(len(inner)):
            # result[i][0] - Table name
            # result[i][1] - CREATE TABLE parameters
            #Searching for CONSTRAINT `tablename_ibfk_1`
                for m in re.finditer('(ibfk_[0-9]+)', inner[j][1]):
                    key = "`" + inner[j][0] + "_" + m.group() + "`"
                    c.execute("ALTER TABLE " + inner[j][0] + " DROP FOREIGN KEY " + key)
                self.commit()
    #end drop_referential_inegrity

    def recreate_tables(self):
        """(Re-)creates the tables of the current DB"""

        self.drop_tables()
        self.resetCache()
        self.resetBulkCache()
        self.create_tables()
        self.createAllIndexes()
        self.commit()
        self.get_sites()
        log.info(_("Finished recreating tables"))
    #end def recreate_tables

    def create_tables(self):
        log.debug(self.sql.query['createSettingsTable'])
        c = self.get_cursor()
        c.execute(self.sql.query['createSettingsTable'])

        log.debug("Creating tables")
        c.execute(self.sql.query['createActionsTable'])
        c.execute(self.sql.query['createRankTable'])
        c.execute(self.sql.query['createStartCardsTable'])
        c.execute(self.sql.query['createSitesTable'])
        c.execute(self.sql.query['createGametypesTable'])
        c.execute(self.sql.query['createFilesTable'])
        c.execute(self.sql.query['createPlayersTable'])
        c.execute(self.sql.query['createAutoratesTable'])
        c.execute(self.sql.query['createWeeksCacheTable'])
        c.execute(self.sql.query['createMonthsCacheTable'])
        c.execute(self.sql.query['createSessionsCacheTable'])
        c.execute(self.sql.query['createTourneyTypesTable'])
        c.execute(self.sql.query['createTourneysTable'])
        c.execute(self.sql.query['createTourneysPlayersTable'])
        c.execute(self.sql.query['createCashCacheTable'])
        c.execute(self.sql.query['createTourCacheTable'])
        c.execute(self.sql.query['createHandsTable'])
        c.execute(self.sql.query['createHandsPlayersTable'])
        c.execute(self.sql.query['createHandsActionsTable'])
        c.execute(self.sql.query['createHandsStoveTable'])
        c.execute(self.sql.query['createHandsPotsTable'])
        c.execute(self.sql.query['createHudCacheTable'])
        c.execute(self.sql.query['createCardsCacheTable'])
        c.execute(self.sql.query['createPositionsCacheTable'])
        c.execute(self.sql.query['createBoardsTable'])
        c.execute(self.sql.query['createBackingsTable'])
        c.execute(self.sql.query['createRawHands'])
        c.execute(self.sql.query['createRawTourneys'])

        # Create unique indexes:
        log.debug("Creating unique indexes")
        c.execute(self.sql.query['addTourneyIndex'])
        c.execute(self.sql.query['addHandsIndex'].replace('<heroseat>', ', heroSeat' if self.publicDB else ''))
        c.execute(self.sql.query['addPlayersIndex'])
        c.execute(self.sql.query['addTPlayersIndex'])
        c.execute(self.sql.query['addPlayersSeat'])
        c.execute(self.sql.query['addHeroSeat'])
        c.execute(self.sql.query['addStartCardsIndex'])
        c.execute(self.sql.query['addActiveSeatsIndex'])
        c.execute(self.sql.query['addPositionIndex'])
        c.execute(self.sql.query['addFilesIndex'])
        c.execute(self.sql.query['addPlayerCharsIndex'])
        c.execute(self.sql.query['addPlayerHeroesIndex'])
        c.execute(self.sql.query['addStartCashIndex'])
        c.execute(self.sql.query['addEffStackIndex'])
        c.execute(self.sql.query['addTotalProfitIndex'])
        c.execute(self.sql.query['addWinningsIndex'])
        c.execute(self.sql.query['addShowdownPotIndex'])
        c.execute(self.sql.query['addStreetIndex'])
        c.execute(self.sql.query['addStreetIdIndex'])
        c.execute(self.sql.query['addCashCacheCompundIndex'])
        c.execute(self.sql.query['addTourCacheCompundIndex'])
        c.execute(self.sql.query['addHudCacheCompundIndex'])
        c.execute(self.sql.query['addCardsCacheCompundIndex'])
        c.execute(self.sql.query['addPositionsCacheCompundIndex'])

        self.fillDefaultData()
        self.commit()

    def drop_tables(self):
        """Drops the fpdb tables from the current db"""
        c = self.get_cursor()

        backend = self.get_backend_name()
        if backend == 'MySQL InnoDB': # what happens if someone is using MyISAM?
            try:
                self.drop_referential_integrity() # needed to drop tables with foreign keys
                c.execute(self.sql.query['list_tables'])
                tables = c.fetchall()
                for table in tables:
                    c.execute(self.sql.query['drop_table'] + table[0])
                c.execute('SET FOREIGN_KEY_CHECKS=1')
            except:
                err = traceback.extract_tb(sys.exc_info()[2])[-1]
                print _("***Error dropping tables:"), +err[2]+"("+str(err[1])+"): "+str(sys.exc_info()[1])
                self.rollback()
        elif backend == 'PostgreSQL':
            try:
                self.commit()
                c.execute(self.sql.query['list_tables'])
                tables = c.fetchall()
                for table in tables:
                    c.execute(self.sql.query['drop_table'] + table[0] + ' cascade')
            except:
                err = traceback.extract_tb(sys.exc_info()[2])[-1]
                print _("***Error dropping tables:"), err[2]+"("+str(err[1])+"): "+str(sys.exc_info()[1])
                self.rollback()
        elif backend == 'SQLite':
            c.execute(self.sql.query['list_tables'])
            for table in c.fetchall():
                if table[0] != 'sqlite_stat1':
                    log.info("%s '%s'" % (self.sql.query['drop_table'], table[0]))
                    c.execute(self.sql.query['drop_table'] + table[0])
        self.commit()
    #end def drop_tables

    def createAllIndexes(self):
        """Create new indexes"""

        if self.backend == self.PGSQL:
            self.connection.set_isolation_level(0)   # allow table/index operations to work
        c = self.get_cursor()
        for idx in self.indexes[self.backend]:
            log.info(_("Creating index %s %s") %(idx['tab'], idx['col']))
            if self.backend == self.MYSQL_INNODB:
                s = "CREATE INDEX %s ON %s(%s)" % (idx['col'],idx['tab'],idx['col'])
                c.execute(s)
            elif self.backend == self.PGSQL or self.backend == self.SQLITE:
                s = "CREATE INDEX %s_%s_idx ON %s(%s)" % (idx['tab'], idx['col'], idx['tab'], idx['col'])
                c.execute(s)

        if self.backend == self.PGSQL:
            self.connection.set_isolation_level(1)   # go back to normal isolation level
    #end def createAllIndexes

    def dropAllIndexes(self):
        """Drop all standalone indexes (i.e. not including primary keys or foreign keys)
           using list of indexes in indexes data structure"""
        # maybe upgrade to use data dictionary?? (but take care to exclude PK and FK)
        if self.backend == self.PGSQL:
            self.connection.set_isolation_level(0)   # allow table/index operations to work
        for idx in self.indexes[self.backend]:
            if self.backend == self.MYSQL_INNODB:
                print (_("Dropping index:"), idx['tab'], idx['col'])
                try:
                    self.get_cursor().execute( "alter table %s drop index %s"
                                             , (idx['tab'], idx['col']) )
                except:
                    print _("Drop index failed:"), str(sys.exc_info())
            elif self.backend == self.PGSQL:
                print (_("Dropping index:"), idx['tab'], idx['col'])
                # mod to use tab_col for index name?
                try:
                    self.get_cursor().execute( "drop index %s_%s_idx"
                                               % (idx['tab'],idx['col']) )
                except:
                    print (_("Drop index failed:"), str(sys.exc_info()))
            elif self.backend == self.SQLITE:
                print (_("Dropping index:"), idx['tab'], idx['col'])
                try:
                    self.get_cursor().execute( "drop index %s_%s_idx"
                                               % (idx['tab'],idx['col']) )
                except:
                    print _("Drop index failed:"), str(sys.exc_info())
            else:
                return -1
        if self.backend == self.PGSQL:
            self.connection.set_isolation_level(1)   # go back to normal isolation level
    #end def dropAllIndexes

    def createAllForeignKeys(self):
        """Create foreign keys"""

        try:
            if self.backend == self.PGSQL:
                self.connection.set_isolation_level(0)   # allow table/index operations to work
            c = self.get_cursor()
        except:
            print _("set_isolation_level failed:"), str(sys.exc_info())

        for fk in self.foreignKeys[self.backend]:
            if self.backend == self.MYSQL_INNODB:
                c.execute("SELECT constraint_name " +
                          "FROM information_schema.KEY_COLUMN_USAGE " +
                          #"WHERE REFERENCED_TABLE_SCHEMA = 'fpdb'
                          "WHERE 1=1 " +
                          "AND table_name = %s AND column_name = %s " +
                          "AND referenced_table_name = %s " +
                          "AND referenced_column_name = %s ",
                          (fk['fktab'], fk['fkcol'], fk['rtab'], fk['rcol']) )
                cons = c.fetchone()
                #print "afterbulk: cons=", cons
                if cons:
                    pass
                else:
                    print _("Creating foreign key:"), fk['fktab'], fk['fkcol'], "->", fk['rtab'], fk['rcol']
                    try:
                        c.execute("alter table " + fk['fktab'] + " add foreign key ("
                                  + fk['fkcol'] + ") references " + fk['rtab'] + "("
                                  + fk['rcol'] + ")")
                    except:
                        print _("Create foreign key failed:"), str(sys.exc_info())
            elif self.backend == self.PGSQL:
                print _("Creating foreign key:"), fk['fktab'], fk['fkcol'], "->", fk['rtab'], fk['rcol']
                try:
                    c.execute("alter table " + fk['fktab'] + " add constraint "
                              + fk['fktab'] + '_' + fk['fkcol'] + '_fkey'
                              + " foreign key (" + fk['fkcol']
                              + ") references " + fk['rtab'] + "(" + fk['rcol'] + ")")
                except:
                    print _("Create foreign key failed:"), str(sys.exc_info())
            else:
                pass

        try:
            if self.backend == self.PGSQL:
                self.connection.set_isolation_level(1)   # go back to normal isolation level
        except:
            print _("set_isolation_level failed:"), str(sys.exc_info())
    #end def createAllForeignKeys

    def dropAllForeignKeys(self):
        """Drop all standalone indexes (i.e. not including primary keys or foreign keys)
           using list of indexes in indexes data structure"""
        # maybe upgrade to use data dictionary?? (but take care to exclude PK and FK)
        if self.backend == self.PGSQL:
            self.connection.set_isolation_level(0)   # allow table/index operations to work
        c = self.get_cursor()

        for fk in self.foreignKeys[self.backend]:
            if self.backend == self.MYSQL_INNODB:
                c.execute("SELECT constraint_name " +
                          "FROM information_schema.KEY_COLUMN_USAGE " +
                          #"WHERE REFERENCED_TABLE_SCHEMA = 'fpdb'
                          "WHERE 1=1 " +
                          "AND table_name = %s AND column_name = %s " +
                          "AND referenced_table_name = %s " +
                          "AND referenced_column_name = %s ",
                          (fk['fktab'], fk['fkcol'], fk['rtab'], fk['rcol']) )
                cons = c.fetchone()
                #print "preparebulk find fk: cons=", cons
                if cons:
                    print _("Dropping foreign key:"), cons[0], fk['fktab'], fk['fkcol']
                    try:
                        c.execute("alter table " + fk['fktab'] + " drop foreign key " + cons[0])
                    except:
                        print _("Warning:"), _("Drop foreign key %s_%s_fkey failed: %s, continuing ...") \
                                  % (fk['fktab'], fk['fkcol'], str(sys.exc_value).rstrip('\n') )
            elif self.backend == self.PGSQL:
#    DON'T FORGET TO RECREATE THEM!!
                print _("Dropping foreign key:"), fk['fktab'], fk['fkcol']
                try:
                    # try to lock table to see if index drop will work:
                    # hmmm, tested by commenting out rollback in grapher. lock seems to work but
                    # then drop still hangs :-(  does work in some tests though??
                    # will leave code here for now pending further tests/enhancement ...
                    c.execute("BEGIN TRANSACTION")
                    c.execute( "lock table %s in exclusive mode nowait" % (fk['fktab'],) )
                    #print "after lock, status:", c.statusmessage
                    #print "alter table %s drop constraint %s_%s_fkey" % (fk['fktab'], fk['fktab'], fk['fkcol'])
                    try:
                        c.execute("alter table %s drop constraint %s_%s_fkey" % (fk['fktab'], fk['fktab'], fk['fkcol']))
                        print _("dropped foreign key %s_%s_fkey, continuing ...") % (fk['fktab'], fk['fkcol'])
                    except:
                        if "does not exist" not in str(sys.exc_value):
                            print _("Warning:"), _("Drop foreign key %s_%s_fkey failed: %s, continuing ...") \
                                  % (fk['fktab'], fk['fkcol'], str(sys.exc_value).rstrip('\n') )
                    c.execute("END TRANSACTION")
                except:
                    print _("Warning:"), _("constraint %s_%s_fkey not dropped: %s, continuing ...") \
                          % (fk['fktab'],fk['fkcol'], str(sys.exc_value).rstrip('\n'))
            else:
                #print _("Only MySQL and Postgres supported so far")
                pass

        if self.backend == self.PGSQL:
            self.connection.set_isolation_level(1)   # go back to normal isolation level
    #end def dropAllForeignKeys


    def fillDefaultData(self):
        c = self.get_cursor()
        c.execute("INSERT INTO Settings (version) VALUES (%s);" % (DB_VERSION))
        #Fill Sites
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('1', 'Full Tilt Poker', 'FT')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('2', 'PokerStars', 'PS')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('3', 'Everleaf', 'EV')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('4', 'Boss', 'BM')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('5', 'OnGame', 'OG')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('6', 'UltimateBet', 'UB')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('7', 'Betfair', 'BF')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('8', 'Absolute', 'AB')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('9', 'PartyPoker', 'PP')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('10', 'PacificPoker', 'P8')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('11', 'Partouche', 'PA')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('12', 'Merge', 'MN')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('13', 'PKR', 'PK')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('14', 'iPoker', 'IP')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('15', 'Winamax', 'WM')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('16', 'Everest', 'EP')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('17', 'Cake', 'CK')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('18', 'Entraction', 'TR')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('19', 'BetOnline', 'BO')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('20', 'Microgaming', 'MG')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('21', 'Bovada', 'BV')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('22', 'Enet', 'EN')")
        c.execute("INSERT INTO Sites (id,name,code) VALUES ('23', 'SealsWithClubs', 'SW')")
        #Fill Actions
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('1', 'ante', 'A')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('2', 'small blind', 'SB')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('3', 'secondsb', 'SSB')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('4', 'big blind', 'BB')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('5', 'both', 'SBBB')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('6', 'calls', 'C')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('7', 'raises', 'R')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('8', 'bets', 'B')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('9', 'stands pat', 'S')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('10', 'folds', 'F')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('11', 'checks', 'K')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('12', 'discards', 'D')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('13', 'bringin', 'I')")
        c.execute("INSERT INTO Actions (id,name,code) VALUES ('14', 'completes', 'P')")
        #Fill Rank
        c.execute("INSERT INTO Rank (id,name) VALUES ('1', 'Nothing')")
        c.execute("INSERT INTO Rank (id,name) VALUES ('2', 'NoPair')")
        c.execute("INSERT INTO Rank (id,name) VALUES ('3', 'OnePair')")
        c.execute("INSERT INTO Rank (id,name) VALUES ('4', 'TwoPair')")
        c.execute("INSERT INTO Rank (id,name) VALUES ('5', 'Trips')")
        c.execute("INSERT INTO Rank (id,name) VALUES ('6', 'Straight')")
        c.execute("INSERT INTO Rank (id,name) VALUES ('7', 'Flush')")
        c.execute("INSERT INTO Rank (id,name) VALUES ('8', 'FlHouse')")
        c.execute("INSERT INTO Rank (id,name) VALUES ('9', 'Quads')")
        c.execute("INSERT INTO Rank (id,name) VALUES ('10', 'StFlush')")
        #Fill StartCards
        sql = "INSERT INTO StartCards (category, name, rank, combinations) VALUES (%s, %s, %s, %s)".replace('%s', self.sql.query['placeholder'])
        for i in range(170):
            (name, rank, combinations) = Card.StartCardRank(i)
            c.execute(sql,  ('holdem', name, rank, combinations))
        for idx in range(-13,1183):
            name = Card.decodeRazzStartHand(idx)
            c.execute(sql, ('razz', name, idx, 0))        

    #end def fillDefaultData

    def rebuild_indexes(self, start=None):
        self.dropAllIndexes()
        self.createAllIndexes()
        self.dropAllForeignKeys()
        self.createAllForeignKeys()
    #end def rebuild_indexes
    
    def replace_statscache(self, type, table, query):
        if table == 'HudCache':
            insert = """HudCache
                (gametypeId
                ,playerId
                ,activeSeats
                ,position
                <tourney_insert_clause>
                ,styleKey"""
    
            select = """h.gametypeId
                      ,hp.playerId
                      ,h.seats as seat_num
                      <hc_position>
                      <tourney_select_clause>
                      ,<styleKey>"""
                          
            group = """h.gametypeId
                        ,hp.playerId
                        ,seat_num
                        ,hc_position
                        <tourney_group_clause>
                        <styleKeyGroup>"""
                        
            query = query.replace('<insert>', insert)
            query = query.replace('<select>', select)
            query = query.replace('<group>', group)
            query = query.replace('<sessions_join_clause>', "")
        
            query = query.replace('<hc_position>', """,case when hp.position = 'B' then 'B'
                        when hp.position = 'S' then 'S'
                        when hp.position = '0' then 'D'
                        when hp.position = '1' then 'C'
                        when hp.position = '2' then 'M'
                        when hp.position = '3' then 'M'
                        when hp.position = '4' then 'M'
                        when hp.position = '5' then 'E'
                        when hp.position = '6' then 'E'
                        when hp.position = '7' then 'E'
                        when hp.position = '8' then 'E'
                        when hp.position = '9' then 'E'
                        else 'E'
                   end                                            as hc_position""")
            if self.backend == self.PGSQL:
                query = query.replace('<styleKey>', "'d' || to_char(h.startTime, 'YYMMDD')")
                query = query.replace('<styleKeyGroup>', ",to_char(h.startTime, 'YYMMDD')")
            elif self.backend == self.SQLITE:
                query = query.replace('<styleKey>', "'d' || substr(strftime('%Y%m%d', h.startTime),3,7)")
                query = query.replace('<styleKeyGroup>', ",substr(strftime('%Y%m%d', h.startTime),3,7)")
            elif self.backend == self.MYSQL_INNODB:
                query = query.replace('<styleKey>', "date_format(h.startTime, 'd%y%m%d')")
                query = query.replace('<styleKeyGroup>', ",date_format(h.startTime, 'd%y%m%d')")
            
            if type == 'tour':
                query = query.replace('<tourney_insert_clause>', ",tourneyTypeId")
                query = query.replace('<tourney_select_clause>', ",t.tourneyTypeId")
                query = query.replace('<tourney_group_clause>', ",t.tourneyTypeId")
            else:
                query = query.replace('<tourney_insert_clause>', "")
                query = query.replace('<tourney_select_clause>', "")
                query = query.replace('<tourney_group_clause>', "")
            
            query = query.replace('<hero_where>', "")
            query = query.replace('<hero_join>', '')
                
        elif table == 'CardsCache':
            insert = """CardsCache
                (weekId
                ,monthId
                <type_insert_clause>
                ,playerId
                ,streetId
                ,boardId
                ,hiLo
                ,startCards
                ,rankId"""
    
            select = """s.weekId
                      ,s.monthId 
                      <type_select_clause>
                      ,hp.playerId
                      ,hs.streetId
                      ,hs.boardId
                      ,hs.hiLo
                      ,case when hs.streetId = 0 then hp.startCards else 170 end as start_cards
                      ,hs.rankId"""
                          
            group = """s.weekId
                        ,s.monthId 
                        <type_group_clause>
                        ,hp.playerId
                        ,hs.streetId
                        ,hs.boardId
                        ,hs.hiLo
                        ,start_cards
                        ,hs.rankId"""
                        
            query = query.replace('<insert>', insert)
            query = query.replace('<select>', select)
            query = query.replace('<group>', group)
            query = query.replace('<hero_join>', ' AND h.heroSeat = hp.seatNo')
            query = query.replace('<sessions_join_clause>', """INNER JOIN SessionsCache s ON (s.id = h.sessionId)
                INNER JOIN Players p ON (hp.playerId = p.id)
                INNER JOIN HandsStove hs ON (hp.playerId = hs.playerId AND hp.handId = hs.handId)""")
            query = query.replace('<hero_where>', '')
            if type=='ring':
                query = query.replace('<type_insert_clause>', ",gametypeId")
                query = query.replace('<type_select_clause>', ",h.gametypeId")
                query = query.replace('<type_group_clause>', ",h.gametypeId")
            else:
                query = query.replace('<type_insert_clause>', ",tourneyTypeId")
                query = query.replace('<type_select_clause>', ",t.tourneyTypeId")
                query = query.replace('<type_group_clause>', ",t.tourneyTypeId") 
                
        elif table == 'PositionsCache':
            insert = """PositionsCache
                (weekId
                ,monthId
                <type_insert_clause>
                ,playerId
                ,activeSeats
                ,position"""
    
            select = """s.weekId
                      ,s.monthId 
                      <type_select_clause>
                      ,hp.playerId
                      ,h.seats as seat_num
                      <pc_position>"""
                          
            group = """s.weekId
                        ,s.monthId 
                        <type_group_clause>
                        ,hp.playerId
                        ,seat_num
                        ,pc_position"""
                        
            query = query.replace('<insert>', insert)
            query = query.replace('<select>', select)
            query = query.replace('<group>', group)
            query = query.replace('<hero_join>', '')
            query = query.replace('<sessions_join_clause>', """INNER JOIN SessionsCache s ON (s.id = h.sessionId)
                INNER JOIN Players p ON (hp.playerId = p.id)""")
            query = query.replace('<pc_position>', """,case when h.heroSeat = hp.seatNo then hp.position else 'N' end as pc_position""")
            query = query.replace('<hero_where>', "")
            
            if type=='ring':
                query = query.replace('<type_insert_clause>', ",gametypeId")
                query = query.replace('<type_select_clause>', ",h.gametypeId")
                query = query.replace('<type_group_clause>', ",h.gametypeId")
            else:
                query = query.replace('<type_insert_clause>', ",tourneyTypeId")
                query = query.replace('<type_select_clause>', ",t.tourneyTypeId")
                query = query.replace('<type_group_clause>', ",t.tourneyTypeId")
                
        return query

    def rebuild_cache(self, h_start=None, v_start=None, table = 'HudCache', ttid = None, wmid = None):
        """clears hudcache and rebuilds from the individual handsplayers records"""
        stime = time()
        # derive list of program owner's player ids
        self.hero = {}                               # name of program owner indexed by site id
        self.hero_ids = {'dummy':-53, 'dummy2':-52}  # playerid of owner indexed by site id
                                                     # make sure at least two values in list
                                                     # so that tuple generation creates doesn't use
                                                     # () or (1,) style
        if not h_start and not v_start:
            self.hero_ids = None
        else:
            for site in self.config.get_supported_sites():
                result = self.get_site_id(site)
                if result:
                    site_id = result[0][0]
                    self.hero[site_id] = self.config.supported_sites[site].screen_name
                    p_id = self.get_player_id(self.config, site, self.hero[site_id])
                    if p_id:
                        self.hero_ids[site_id] = int(p_id)
                        
            if not h_start:
                h_start = self.hero_hudstart_def
            if not v_start:
                v_start = self.villain_hudstart_def
                
        if not ttid and not wmid:
            self.get_cursor().execute(self.sql.query['clear%s' % table])
            self.commit()
        
        if not ttid:
            if self.hero_ids is None:
                if wmid:
                    where = "WHERE g.type = 'ring' AND weekId = %s and monthId = %s<hero_where>" % wmid
                else:
                    where = "WHERE g.type = 'ring'<hero_where>"
            else:
                where =   "where (((    hp.playerId not in " + str(tuple(self.hero_ids.values())) \
                        + "       and h.startTime > '" + v_start + "')" \
                        + "   or (    hp.playerId in " + str(tuple(self.hero_ids.values())) \
                        + "       and h.startTime > '" + h_start + "'))" \
                        + "   AND hp.tourneysPlayersId IS NULL)"
            rebuild_sql_cash = self.sql.query['rebuildCache'].replace('%s', self.sql.query['placeholder'])
            rebuild_sql_cash = rebuild_sql_cash.replace('<tourney_join_clause>', "")
            rebuild_sql_cash = rebuild_sql_cash.replace('<where_clause>', where)
            rebuild_sql_cash = self.replace_statscache('ring', table, rebuild_sql_cash)
            #print rebuild_sql_cash 
            self.get_cursor().execute(rebuild_sql_cash)
            self.commit()
            #print _("Rebuild cache(cash) took %.1f seconds") % (time() - stime,)

        if ttid:
            where = "WHERE t.tourneyTypeId = %s<hero_where>" % ttid
        elif self.hero_ids is None:
            if wmid:
                where = "WHERE g.type = 'tour' AND weekId = %s and monthId = %s<hero_where>" % wmid
            else:
                where = "WHERE g.type = 'tour'<hero_where>"
        else:
            where =   "where (((    hp.playerId not in " + str(tuple(self.hero_ids.values())) \
                    + "       and h.startTime > '" + v_start + "')" \
                    + "   or (    hp.playerId in " + str(tuple(self.hero_ids.values())) \
                    + "       and h.startTime > '" + h_start + "'))" \
                    + "   AND hp.tourneysPlayersId >= 0)"
        rebuild_sql_tourney = self.sql.query['rebuildCache'].replace('%s', self.sql.query['placeholder'])
        rebuild_sql_tourney = rebuild_sql_tourney.replace('<tourney_join_clause>', """INNER JOIN Tourneys t ON (t.id = h.tourneyId)""")
        rebuild_sql_tourney = rebuild_sql_tourney.replace('<where_clause>', where)
        rebuild_sql_tourney = self.replace_statscache('tour', table, rebuild_sql_tourney)
        #print rebuild_sql_tourney
        self.get_cursor().execute(rebuild_sql_tourney)
        self.commit()
        #print _("Rebuild hudcache took %.1f seconds") % (time() - stime,)
    #end def rebuild_cache
    
    def update_timezone(self, tz_name):
        select_WC     = self.sql.query['select_WC'].replace('%s', self.sql.query['placeholder'])
        select_MC     = self.sql.query['select_MC'].replace('%s', self.sql.query['placeholder'])
        insert_WC     = self.sql.query['insert_WC'].replace('%s', self.sql.query['placeholder'])
        insert_MC     = self.sql.query['insert_MC'].replace('%s', self.sql.query['placeholder'])
        update_WM_SC  = self.sql.query['update_WM_SC'].replace('%s', self.sql.query['placeholder'])
        c = self.get_cursor()
        c.execute("SELECT id, sessionStart, weekId wid, monthId mid FROM SessionsCache")
        sessions = self.fetchallDict(c)
        for s in sessions:
            utc_start = pytz.utc.localize(s['sessionStart'])
            tz = pytz.timezone(tz_name)
            loc_tz = utc_start.astimezone(tz).strftime('%z')
            offset = timedelta(hours=int(loc_tz[:-2]), minutes=int(loc_tz[0]+loc_tz[-2:]))
            local = s['sessionStart'] + offset
            monthStart = datetime(local.year, local.month, 1)
            weekdate   = datetime(local.year, local.month, local.day) 
            weekStart  = weekdate - timedelta(days=weekdate.weekday())
            wid = self.insertOrUpdate('weeks', c, (weekStart,), select_WC, insert_WC)
            mid = self.insertOrUpdate('months', c, (monthStart,), select_MC, insert_MC)
            if wid != s['wid'] or mid != s['mid']:
                row = [wid, mid, s['id']]
                c.execute(update_WM_SC, row)
                self.wmold.add((s['wid'], s['mid']))
                self.wmnew.add((wid, mid))
        self.commit()
        self.cleanUpWeeksMonths()
    
    def rebuild_sessionscache(self, tz_name = None, recreate=False, heroes = []):
        """clears sessionscache and rebuilds from the individual records"""
        c = self.get_cursor()
        if not heroes:
            c.execute("SELECT id FROM Players WHERE hero=1")
            herorecords_cash = c.fetchall()
            for h in herorecords_cash:
                heroes += h                                
        rebuildSessionsCache = self.sql.query['rebuildSessionsCache']
        if len(heroes) == 0:
            where         = '0'
            where_summary = '0'
        elif len(heroes) > 0:
            where         = str(heroes[0])
            where_summary = str(heroes[0])
            if len(heroes) > 1:
                for i in heroes:
                    if i != heroes[0]:
                        where = where + ' OR HandsPlayers.playerId = %s' % str(i)
        rebuildSessionsCache     = rebuildSessionsCache.replace('<where_clause>', where)
        rebuildSessionsCacheRing = rebuildSessionsCache.replace('<tourney_join_clause>','')
        rebuildSessionsCacheRing = rebuildSessionsCacheRing.replace('<tourney_type_clause>','NULL,')
        rebuildSessionsCacheTour = rebuildSessionsCache.replace('<tourney_join_clause>',"""INNER JOIN Tourneys ON (Tourneys.id = Hands.tourneyId)""")
        rebuildSessionsCacheTour = rebuildSessionsCacheTour.replace('<tourney_type_clause>','HandsPlayers.tourneysPlayersId,')
        rebuildSessionsCacheRing = rebuildSessionsCacheRing.replace('%s', self.sql.query['placeholder'])
        rebuildSessionsCacheTour = rebuildSessionsCacheTour.replace('%s', self.sql.query['placeholder'])

        queries, type = [rebuildSessionsCacheTour, rebuildSessionsCacheRing], ['tour', 'ring']
        c = self.get_cursor()
        c.execute("SELECT count(H.id) FROM Hands H")
        max = c.fetchone()[0]
        c.execute(self.sql.query['clear_SC_H'])
        c.execute(self.sql.query['clear_SC_T'])
        c.execute(self.sql.query['clear_SC_CC'])
        c.execute(self.sql.query['clear_SC_TC'])
        c.execute(self.sql.query['clearCashCache'])
        c.execute(self.sql.query['clearTourCache'])
        c.execute(self.sql.query['clearSessionsCache'])
        if not recreate:
            c.execute(self.sql.query['clearWeeksCache'])
            c.execute(self.sql.query['clearMonthsCache'])
        else:
            if self.backend == self.MYSQL_INNODB:
                c.execute('SET FOREIGN_KEY_CHECKS=0')
            c.execute('DROP TABLE IF EXISTS CashCache, TourCache, SessionsCache, WeeksCache, MonthsCache')
            if self.backend == self.MYSQL_INNODB:
                c.execute('SET FOREIGN_KEY_CHECKS=1')
            c.execute(self.sql.query['createWeeksCacheTable'])
            c.execute(self.sql.query['createMonthsCacheTable'])
            c.execute(self.sql.query['createSessionsCacheTable'])
            c.execute(self.sql.query['createCashCacheTable'])
            c.execute(self.sql.query['createTourCacheTable'])       
        self.commit()
        
        start, end, limit =  0, 5000, 5000
        print max, 'total hands'
        while start < max:
            ttime = time()
            t, r = 0, 0
            for k in range(2):
                hid = {}
                self.resetBulkCache()
                c.execute(queries[k], (type[k], start, end))
                tmp = c.fetchone()
                if tmp and type[k]=='ring': r += 1
                if tmp and type[k]=='tour': t += 1
                while tmp:
                    pids, game, pdata = {}, {}, {}
                    pdata['pname'] = {}
                    id                                    = tmp[0]
                    startTime                             = tmp[1]
                    pids['pname']                         = tmp[2]
                    tid                                   = tmp[3]
                    gtid                                  = tmp[4]
                    game['type']                          = tmp[5]
                    pdata['pname']['tourneysPlayersIds']  = tmp[6]
                    pdata['pname']['totalProfit']         = tmp[7]
                    pdata['pname']['rake']                = tmp[8]
                    pdata['pname']['allInEV']             = tmp[9]
                    pdata['pname']['street0VPI']          = tmp[10]
                    pdata['pname']['street1Seen']         = tmp[11]
                    pdata['pname']['sawShowdown']         = tmp[12]
                    tmp = c.fetchone()
                    hid[id] = tid
                    self.storeSessionsCache (id, pids, startTime, heroes, tz_name, tmp == None)
                    self.storeCashCache(id, pids, startTime, gtid, game, pdata, heroes, tmp == None)
                    self.storeTourCache(id, pids, tid, startTime, game, pdata, heroes, tmp == None)
                    if tmp == None:
                        for i, id in self.sc.iteritems():
                            if i!='bk':
                                sid =  id['id']
                                if hid[i]: 
                                    self.tbulk[hid[i]] = sid
                                    gid = None
                                else: gid = self.gc[i]['id']
                                q = self.sql.query['update_RSC_H']
                                q = q.replace('%s', self.sql.query['placeholder'])
                                c.execute(q, (sid, gid, i))
                        self.updateTourneysSessions()
                        self.commit()
                        break
                    if type[k]=='ring': r += 1
                    if type[k]=='tour': t += 1
            print 'Hand ids', start, '-', end, '\t', 'total', (r+t), 'ring:', r, 'tour:', t, '\t', int(time() - ttime), 'sec',  int((r+t)/(time() - ttime)), 'hands/sec'
            start += limit
            end   += limit
            self.commit()
        self.commit()
       

    def get_hero_hudcache_start(self):
        """fetches earliest stylekey from hudcache for one of hero's player ids"""

        try:
            # derive list of program owner's player ids
            self.hero = {}                               # name of program owner indexed by site id
            self.hero_ids = {'dummy':-53, 'dummy2':-52}  # playerid of owner indexed by site id
                                                         # make sure at least two values in list
                                                         # so that tuple generation creates doesn't use
                                                         # () or (1,) style
            for site in self.config.get_supported_sites():
                result = self.get_site_id(site)
                if result:
                    site_id = result[0][0]
                    self.hero[site_id] = self.config.supported_sites[site].screen_name
                    p_id = self.get_player_id(self.config, site, self.hero[site_id])
                    if p_id:
                        self.hero_ids[site_id] = int(p_id)

            q = self.sql.query['get_hero_hudcache_start'].replace("<playerid_list>", str(tuple(self.hero_ids.values())))
            c = self.get_cursor()
            c.execute(q)
            tmp = c.fetchone()
            if tmp == (None,):
                return self.hero_hudstart_def
            else:
                return "20"+tmp[0][1:3] + "-" + tmp[0][3:5] + "-" + tmp[0][5:7]
        except:
            err = traceback.extract_tb(sys.exc_info()[2])[-1]
            print _("Error rebuilding hudcache:"), str(sys.exc_value)
            print err
    #end def get_hero_hudcache_start


    def analyzeDB(self):
        """Do whatever the DB can offer to update index/table statistics"""
        stime = time()
        if self.backend == self.MYSQL_INNODB or self.backend == self.SQLITE:
            try:
                self.get_cursor().execute(self.sql.query['analyze'])
            except:
                print _("Error during analyze:"), str(sys.exc_value)
        elif self.backend == self.PGSQL:
            self.connection.set_isolation_level(0)   # allow analyze to work
            try:
                self.get_cursor().execute(self.sql.query['analyze'])
            except:
                print _("Error during analyze:"), str(sys.exc_value)
            self.connection.set_isolation_level(1)   # go back to normal isolation level
        self.commit()
        atime = time() - stime
        log.info(_("Analyze took %.1f seconds") % (atime,))
    #end def analyzeDB

    def vacuumDB(self):
        """Do whatever the DB can offer to update index/table statistics"""
        stime = time()
        if self.backend == self.MYSQL_INNODB or self.backend == self.SQLITE:
            try:
                self.get_cursor().execute(self.sql.query['vacuum'])
            except:
                print _("Error during vacuum:"), str(sys.exc_value)
        elif self.backend == self.PGSQL:
            self.connection.set_isolation_level(0)   # allow vacuum to work
            try:
                self.get_cursor().execute(self.sql.query['vacuum'])
            except:
                print _("Error during vacuum:"), str(sys.exc_value)
            self.connection.set_isolation_level(1)   # go back to normal isolation level
        self.commit()
        atime = time() - stime
        print _("Vacuum took %.1f seconds") % (atime,)
    #end def analyzeDB

# Start of Hand Writing routines. Idea is to provide a mixture of routines to store Hand data
# however the calling prog requires. Main aims:
# - existing static routines from fpdb_simple just modified

    def setThreadId(self, threadid):
        self.threadId = threadid
                
    def acquireLock(self, wait=True, retry_time=.01):
        while not self._has_lock:
            cursor = self.get_cursor()
            num = cursor.execute(self.sql.query['switchLockOn'], (True, self.threadId))
            self.commit()
            if (self.backend == self.MYSQL_INNODB and num == 0):
                if not wait:
                    return False
                sleep(retry_time)
            else:
                self._has_lock = True
                return True
    
    def releaseLock(self):
        if self._has_lock:
            cursor = self.get_cursor()
            num = cursor.execute(self.sql.query['switchLockOff'], (False, self.threadId))
            self.commit()
            self._has_lock = False

    def lock_for_insert(self):
        """Lock tables in MySQL to try to speed inserts up"""
        try:
            self.get_cursor().execute(self.sql.query['lockForInsert'])
        except:
            print _("Error during lock_for_insert:"), str(sys.exc_value)
    #end def lock_for_insert
    
    def resetBulkCache(self, reconnect=False):
        self.siteHandNos = []         # cache of siteHandNo
        self.hbulk       = []         # Hands bulk inserts
        self.bbulk       = []         # Boards bulk inserts
        self.hpbulk      = []         # HandsPlayers bulk inserts
        self.habulk      = []         # HandsActions bulk inserts
        self.hcbulk      = {}         # HudCache bulk inserts
        self.dcbulk      = {}         # CardsCache bulk inserts
        self.pcbulk      = {}         # PositionsCache bulk inserts
        self.hsbulk      = []         # HandsStove bulk inserts
        self.htbulk      = []         # HandsPots bulk inserts
        self.tbulk       = {}         # Tourneys bulk updates
        self.sc          = {'bk': []} # SessionsCache bulk updates
        self.cc          = {}         # CashCache bulk updates
        self.tc          = {}         # TourCache bulk updates
        self.hids        = []         # hand ids in order of hand bulk inserts
        #self.tids        = []         # tourney ids in order of hp bulk inserts
        if reconnect: self.do_connect(self.config)
        
    def executemany(self, c, q, values):
        batch_size=20000 #experiment to find optimal batch_size for your data
        while values: # repeat until all records in values have been inserted ''
            batch, values = values[:batch_size], values[batch_size:] #split values into the current batch and the remaining records
            c.executemany(q, batch ) #insert current batch ''

    def storeHand(self, hdata, doinsert = False, printdata = False):
        if printdata:
            print ("######## Hands ##########")
            import pprint
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(hdata)
            print ("###### End Hands ########")
            
        # Tablename can have odd charachers
        hdata['tableName'] = Charset.to_db_utf8(hdata['tableName'])
        
        self.hids.append(hdata['id'])
        self.hbulk.append( [ hdata['tableName'],
                             hdata['siteHandNo'],
                             hdata['tourneyId'],
                             hdata['gametypeId'],
                             hdata['sessionId'],
                             hdata['fileId'],
                             hdata['startTime'],                
                             datetime.utcnow(), #importtime
                             hdata['seats'],
                             hdata['heroSeat'],
                             hdata['texture'],
                             hdata['playersVpi'],
                             hdata['boardcard1'],
                             hdata['boardcard2'],
                             hdata['boardcard3'],
                             hdata['boardcard4'],
                             hdata['boardcard5'],
                             hdata['runItTwice'],
                             hdata['playersAtStreet1'],
                             hdata['playersAtStreet2'],
                             hdata['playersAtStreet3'],
                             hdata['playersAtStreet4'],
                             hdata['playersAtShowdown'],
                             hdata['street0Raises'],
                             hdata['street1Raises'],
                             hdata['street2Raises'],
                             hdata['street3Raises'],
                             hdata['street4Raises'],
                             hdata['street1Pot'],
                             hdata['street2Pot'],
                             hdata['street3Pot'],
                             hdata['street4Pot'],
                             hdata['showdownPot']
                             ])

        if doinsert:
            self.appendHandsSessionIds()
            self.updateTourneysSessions()
            q = self.sql.query['store_hand']
            q = q.replace('%s', self.sql.query['placeholder'])
            c = self.get_cursor()
            self.executemany(c, q, self.hbulk)
            self.commit()
    
    def storeBoards(self, id, boards, doinsert):
        if boards: 
            for b in boards:
                self.bbulk += [[id] + b]
        if doinsert and self.bbulk:
            q = self.sql.query['store_boards']
            q = q.replace('%s', self.sql.query['placeholder'])
            c = self.get_cursor()
            self.executemany(c, q, self.bbulk) #c.executemany(q, self.bbulk)
    
    def updateTourneysSessions(self):
        if self.tbulk:
            q_update_sessions  = self.sql.query['updateTourneysSessions'].replace('%s', self.sql.query['placeholder'])
            c = self.get_cursor()
            for t, sid in self.tbulk.iteritems():
                c.execute(q_update_sessions,  (sid, t))
                self.commit()

    def storeHandsPlayers(self, hid, pids, pdata, doinsert = False, printdata = False):
        #print "DEBUG: %s %s %s" %(hid, pids, pdata)
        if printdata:
            import pprint
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(pdata)

        hpbulk = self.hpbulk
        for p, pvalue in pdata.iteritems():
            # Add (hid, pids[p]) + all the values in pvalue at the
            # keys in HANDS_PLAYERS_KEYS to hpbulk.
            bulk_data = [pvalue[key] for key in HANDS_PLAYERS_KEYS]
            bulk_data.append(pids[p])
            bulk_data.append(hid)
            bulk_data.reverse()
            hpbulk.append(bulk_data)

        if doinsert:
            #self.appendHandsPlayersSessionIds()
            q = self.sql.query['store_hands_players']
            q = q.replace('%s', self.sql.query['placeholder'])
            c = self.get_cursor(True)
            self.executemany(c, q, self.hpbulk) #c.executemany(q, self.hpbulk)

    #Supporto agli aggiornamenti dei risultati di torneo dai dati
    def storeTourResults(self, tourneyResults, currency, tourneyId, startTime):
        sqlParametersTourneys = {
            'startTime': startTime,
            'tourneyId': tourneyId
        }
        try:
            self.cursor.execute(
                self.sql.query['updateTourneysResults'],
                sqlParametersTourneys)
        except:
            print "Unexpected error:", sys.exc_info()[0]

        for playerresult in tourneyResults:
            sqlParametersTourneysPlayers = {
                'rank': playerresult[1],
                'winnings': playerresult[2],
                'winningscurrency': currency,
                'playerName': playerresult[0],
                'tourneyId': tourneyId
            }
            try:
                self.cursor.execute(
                    self.sql.query['updateTourneysPlayersResults'],
                    sqlParametersTourneysPlayers)
            except:
                print "Unexpected error:", sys.exc_info()[0]

            print "Giocatore {0} in posizione {1} vince {2} valuta {3} in torneo {4} iniziato alle {5}".format(
                playerresult[0],
                playerresult[1],
                playerresult[2],
                currency,
                tourneyId,
                startTime)
            
    def storeHandsPots(self, tdata, doinsert):
        self.htbulk += tdata
        if doinsert and self.htbulk:
            q = self.sql.query['store_hands_pots']
            q = q.replace('%s', self.sql.query['placeholder'])
            c = self.get_cursor()
            self.executemany(c, q, self.htbulk) #c.executemany(q, self.hsbulk)

    def storeHandsActions(self, hid, pids, adata, doinsert = False, printdata = False):
        #print "DEBUG: %s %s %s" %(hid, pids, adata)

        # This can be used to generate test data. Currently unused
        #if printdata:
        #    import pprint
        #    pp = pprint.PrettyPrinter(indent=4)
        #    pp.pprint(adata)
        
        for a in adata:
            self.habulk.append( (hid,
                                 pids[adata[a]['player']],
                                 adata[a]['street'],
                                 adata[a]['actionNo'],
                                 adata[a]['streetActionNo'],
                                 adata[a]['actionId'],
                                 adata[a]['amount'],
                                 adata[a]['raiseTo'],
                                 adata[a]['amountCalled'],
                                 adata[a]['numDiscarded'],
                                 adata[a]['cardsDiscarded'],
                                 adata[a]['allIn']
                               ) )
            
        if doinsert:
            q = self.sql.query['store_hands_actions']
            q = q.replace('%s', self.sql.query['placeholder'])
            c = self.get_cursor()
            self.executemany(c, q, self.habulk) #c.executemany(q, self.habulk)
    
    def storeHandsStove(self, sdata, doinsert):
        self.hsbulk += sdata
        if doinsert and self.hsbulk:
            q = self.sql.query['store_hands_stove']
            q = q.replace('%s', self.sql.query['placeholder'])
            c = self.get_cursor()
            self.executemany(c, q, self.hsbulk) #c.executemany(q, self.hsbulk)
            
    def storeHudCache(self, gid, gametype, pids, starttime, pdata, doinsert=False):
        """Update cached statistics. If update fails because no record exists, do an insert."""
                
        if pdata:   
            tz = datetime.utcnow() - datetime.today()
            tz_offset = tz.seconds/3600
            tz_day_start_offset = self.day_start + tz_offset
            
            d = timedelta(hours=tz_day_start_offset)
            starttime_offset = starttime - d
            styleKey = datetime.strftime(starttime_offset, 'd%y%m%d')
            seats = len(pids)
            
        pos = {'B':'B', 'S':'S', 0:'D', 1:'C', 2:'M', 3:'M', 4:'M', 5:'E', 6:'E', 7:'E', 8:'E', 9:'E' }
            
        for p in pdata:
            player_stats = pdata.get(p)
            garbageTourneyTypes = player_stats['tourneyTypeId'] in self.ttnew or player_stats['tourneyTypeId'] in self.ttold
            if not garbageTourneyTypes:
                position = pos[player_stats['position']]
                k =   (gid
                      ,pids[p]
                      ,seats
                      ,position
                      ,player_stats['tourneyTypeId']
                      ,styleKey
                      )
                player_stats['hands'] = 1
                line = [int(player_stats[s]) for s in CACHE_KEYS]
                    
                hud = self.hcbulk.get(k)
                # Add line to the old line in the hudcache.
                if hud is not None:
                    for idx,val in enumerate(line):
                        hud[idx] += val
                else:
                    self.hcbulk[k] = line
                
        if doinsert:
            update_hudcache = self.sql.query['update_hudcache']
            update_hudcache = update_hudcache.replace('%s', self.sql.query['placeholder'])
            insert_hudcache = self.sql.query['insert_hudcache']
            insert_hudcache = insert_hudcache.replace('%s', self.sql.query['placeholder'])
            
            select_hudcache_ring = self.sql.query['select_hudcache_ring']
            select_hudcache_ring = select_hudcache_ring.replace('%s', self.sql.query['placeholder'])
            select_hudcache_tour = self.sql.query['select_hudcache_tour']
            select_hudcache_tour = select_hudcache_tour.replace('%s', self.sql.query['placeholder'])
            inserts = []
            c = self.get_cursor()
            for k, item in self.hcbulk.iteritems():
                
                if not k[4]:
                    q = select_hudcache_ring
                    row = list(k[:4]) + [k[-1]]
                else:
                    q = select_hudcache_tour
                    row = list(k)
                
                c.execute(q, row)
                result = c.fetchone()
                if result:
                    id = result[0]
                    update = item + [id]
                    c.execute(update_hudcache, update)
                    
                else:
                    inserts.append(list(k) + item)
                
            if inserts:
                c.executemany(insert_hudcache, inserts)
            self.commit()
            
    def storeSessionsCache(self, hid, pids, startTime, heroes, tz_name, doinsert = False):
        """Update cached sessions. If no record exists, do an insert"""
        THRESHOLD     = timedelta(seconds=int(self.sessionTimeout * 60))
        if tz_name in pytz.common_timezones:
            if self.backend == self.SQLITE:
                naive = datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S')
            else:
                naive = startTime.replace(tzinfo=None)
            utc_start = pytz.utc.localize(naive)
            tz = pytz.timezone(tz_name)
            loc_tz = utc_start.astimezone(tz).strftime('%z')
            offset = timedelta(hours=int(loc_tz[:-2]), minutes=int(loc_tz[0]+loc_tz[-2:]))
            local = naive + offset
            monthStart = datetime(local.year, local.month, 1)
            weekdate   = datetime(local.year, local.month, local.day)
            weekStart  = weekdate - timedelta(days=weekdate.weekday())
        else:
            if strftime('%Z') == 'UTC':
                local = startTime
                loc_tz = '0'
            else:
                tz_dt = datetime.today() - datetime.utcnow()
                loc_tz = tz_dt.seconds/3600 - 24
                offset = timedelta(hours=int(loc_tz))
                local = startTime + offset
            monthStart = datetime(local.year, local.month, 1)
            weekdate   = datetime(local.year, local.month, local.day)
            weekStart  = weekdate - timedelta(days=weekdate.weekday())
       
        j, hand = None, {}
        for p, id in pids.iteritems():
            if id in heroes:
                if self.backend == self.SQLITE:
                    hand['startTime']  = datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S')
                    hand['weekStart']  = datetime.strptime(weekStart, '%Y-%m-%d %H:%M:%S')
                    hand['monthStart'] = datetime.strptime(monthStart, '%Y-%m-%d %H:%M:%S')
                else:
                    hand['startTime']  = startTime.replace(tzinfo=None)
                    hand['weekStart']  = weekStart
                    hand['monthStart'] = monthStart
                hand['ids'] = [hid]
        
        id = []
        if hand:
            lower = hand['startTime']-THRESHOLD
            upper = hand['startTime']+THRESHOLD
            for i in range(len(self.sc['bk'])):
                if ((lower  <= self.sc['bk'][i]['sessionEnd'])
                and (upper  >= self.sc['bk'][i]['sessionStart'])):
                    if ((hand['startTime'] <= self.sc['bk'][i]['sessionEnd']) 
                    and (hand['startTime'] >= self.sc['bk'][i]['sessionStart'])):
                         id.append(i)
                    elif hand['startTime'] < self.sc['bk'][i]['sessionStart']:
                         self.sc['bk'][i]['sessionStart'] = hand['startTime']
                         self.sc['bk'][i]['weekStart']    = hand['weekStart']
                         self.sc['bk'][i]['monthStart']   = hand['monthStart']
                         id.append(i)
                    elif hand['startTime'] > self.sc['bk'][i]['sessionEnd']:
                         self.sc['bk'][i]['sessionEnd'] = hand['startTime']
                         id.append(i)
            if len(id) == 1:
                j = id[0]
                self.sc['bk'][j]['ids'] += [hid]
            elif len(id) == 2:
                j, k = id
                if  self.sc['bk'][j]['sessionStart'] < self.sc['bk'][k]['sessionStart']:
                    self.sc['bk'][j]['sessionEnd']   = self.sc['bk'][k]['sessionEnd']
                else:
                    self.sc['bk'][j]['sessionStart'] = self.sc['bk'][k]['sessionStart']
                    self.sc['bk'][j]['weekStart']    = self.sc['bk'][k]['weekStart']
                    self.sc['bk'][j]['monthStart']   = self.sc['bk'][k]['monthStart']
                sh = self.sc['bk'].pop(k)
                self.sc['bk'][j]['ids'] += [hid]
                self.sc['bk'][j]['ids'] += sh['ids']
            elif len(id) == 0:
                j = len(self.sc['bk'])
                hand['id'] = None
                hand['sessionStart'] = hand['startTime']
                hand['sessionEnd']   = hand['startTime']
                self.sc['bk'].append(hand)
        
        if doinsert:
            select_SC     = self.sql.query['select_SC'].replace('%s', self.sql.query['placeholder'])
            select_WC     = self.sql.query['select_WC'].replace('%s', self.sql.query['placeholder'])
            select_MC     = self.sql.query['select_MC'].replace('%s', self.sql.query['placeholder'])
            update_SC     = self.sql.query['update_SC'].replace('%s', self.sql.query['placeholder'])
            insert_WC     = self.sql.query['insert_WC'].replace('%s', self.sql.query['placeholder'])
            insert_MC     = self.sql.query['insert_MC'].replace('%s', self.sql.query['placeholder'])
            insert_SC     = self.sql.query['insert_SC'].replace('%s', self.sql.query['placeholder'])
            update_SC_CC  = self.sql.query['update_SC_CC'].replace('%s', self.sql.query['placeholder'])
            update_SC_TC  = self.sql.query['update_SC_TC'].replace('%s', self.sql.query['placeholder'])
            update_SC_T   = self.sql.query['update_SC_T'].replace('%s', self.sql.query['placeholder'])
            update_SC_H   = self.sql.query['update_SC_H'].replace('%s', self.sql.query['placeholder'])
            delete_SC     = self.sql.query['delete_SC'].replace('%s', self.sql.query['placeholder'])
            c = self.get_cursor()
            for i in range(len(self.sc['bk'])):
                lower = self.sc['bk'][i]['sessionStart'] - THRESHOLD
                upper = self.sc['bk'][i]['sessionEnd']   + THRESHOLD
                c.execute(select_SC, (lower, upper))
                r = self.fetchallDict(c)
                num = len(r)
                if (num == 1):
                    start, end  = r[0]['sessionStart'], r[0]['sessionEnd']
                    week, month = r[0]['weekStart'],    r[0]['monthStart']
                    wid, mid    = r[0]['weekId'],       r[0]['monthId']
                    update, updateW, updateM = False, False, False
                    if self.sc['bk'][i]['sessionStart'] < start:
                        start, update = self.sc['bk'][i]['sessionStart'], True
                        if self.sc['bk'][i]['weekStart'] != week:
                            week, updateW = self.sc['bk'][i]['weekStart'], True
                        if self.sc['bk'][i]['monthStart'] != month:
                            month, updateM = self.sc['bk'][i]['monthStart'], True
                        if (updateW or updateM):
                            self.wmold.add((wid, mid))
                    if self.sc['bk'][i]['sessionEnd'] > end:
                        end, update = self.sc['bk'][i]['sessionEnd'], True
                    if updateW:  wid = self.insertOrUpdate('weeks', c, (week,), select_WC, insert_WC)
                    if updateM:  mid = self.insertOrUpdate('months', c, (month,), select_MC, insert_MC)
                    if (updateW or updateM):
                        self.wmnew.add((wid, mid))
                    if update: 
                        c.execute(update_SC, [wid, mid, start, end, r[0]['id']])
                    for h in  self.sc['bk'][i]['ids']:
                        self.sc[h] = {'id': r[0]['id'], 'wid': wid, 'mid': mid}
                elif (num > 1):
                    start, end, wmold, merge = None, None, set(), []
                    for n in r: merge.append(n['id'])
                    merge.sort()
                    r.append(self.sc['bk'][i])
                    for n in r:
                        if 'weekId' in n:
                            wmold.add((n['weekId'],  n['monthId']))    
                        if start: 
                            if  start > n['sessionStart']: 
                                start = n['sessionStart']
                                week  = n['weekStart']
                                month = n['monthStart']
                        else: 
                            start = n['sessionStart']
                            week  = n['weekStart']
                            month = n['monthStart']
                        if end: 
                            if  end < n['sessionEnd']: 
                                end = n['sessionEnd']
                        else:
                            end = n['sessionEnd']
                    wid = self.insertOrUpdate('weeks', c, (week,), select_WC, insert_WC)
                    mid = self.insertOrUpdate('months', c, (month,), select_MC, insert_MC)
                    wmold.discard((wid, mid))
                    if len(wmold)>0:
                        self.wmold = self.wmold.union(wmold)
                        self.wmnew.add((wid, mid))
                    row = [wid, mid, start, end]
                    c.execute(insert_SC, row)
                    sid = self.get_last_insert_id(c)
                    for h in self.sc['bk'][i]['ids']:
                        self.sc[h] = {'id': sid, 'wid': wid, 'mid': mid}
                    for m in merge:
                        for h, n in self.sc.iteritems():
                            if h!='bk' and n['id'] == m:
                                self.sc[h] = {'id': sid, 'wid': wid, 'mid': mid}
                        c.execute(update_SC_TC,(sid, m))
                        c.execute(update_SC_CC,(sid, m))
                        c.execute(update_SC_T, (sid, m))
                        c.execute(update_SC_H, (sid, m))
                        c.execute(delete_SC, (m,))
                elif (num == 0):
                    start   =  self.sc['bk'][i]['sessionStart']
                    end     =  self.sc['bk'][i]['sessionEnd']
                    week    =  self.sc['bk'][i]['weekStart']
                    month   =  self.sc['bk'][i]['monthStart']
                    wid = self.insertOrUpdate('weeks', c, (week,), select_WC, insert_WC)
                    mid = self.insertOrUpdate('months', c, (month,), select_MC, insert_MC)
                    row = [wid, mid, start, end]
                    c.execute(insert_SC, row)
                    sid = self.get_last_insert_id(c)
                    for h in self.sc['bk'][i]['ids']:
                        self.sc[h] = {'id': sid, 'wid': wid, 'mid': mid}
            self.commit()
    
    def storeCashCache(self, hid, pids, startTime, gtid, gametype, pdata, heroes, hero, doinsert = False):
        """Update cached cash sessions. If no record exists, do an insert"""      
        THRESHOLD    = timedelta(seconds=int(self.sessionTimeout * 60))
        if gametype['type']=='ring' and pdata:
            for p, pid in pids.iteritems():
                hp = {}
                k = (gtid, pid)
                if self.backend == self.SQLITE:
                    hp['startTime'] = datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S')
                else:
                    hp['startTime'] = startTime.replace(tzinfo=None)
                hp['hid']           = hid
                hp['ids']           = []
                pdata[p]['hands']   = 1
                hp['line'] = [int(pdata[p][s]) for s in CACHE_KEYS]
                id = []
                cashplayer = self.cc.get(k)
                if cashplayer is not None:        
                    lower = hp['startTime']-THRESHOLD
                    upper = hp['startTime']+THRESHOLD
                    for i in range(len(cashplayer)):
                        if lower <= cashplayer[i]['endTime'] and upper >= cashplayer[i]['startTime']:
                            if len(id)==0:
                                for idx,val in enumerate(hp['line']):
                                    cashplayer[i]['line'][idx] += val
                            if ((hp['startTime'] <= cashplayer[i]['endTime']) 
                            and (hp['startTime'] >= cashplayer[i]['startTime'])):
                                id.append(i)
                            elif hp['startTime']  <  cashplayer[i]['startTime']:
                                cashplayer[i]['startTime'] = hp['startTime']
                                id.append(i)
                            elif hp['startTime']  >  cashplayer[i]['endTime']:
                                cashplayer[i]['endTime'] = hp['startTime']
                                id.append(i)
                if len(id) == 1:
                    i = id[0]
                    if pids[p]==heroes[0]:
                        self.cc[k][i]['ids'].append(hid)
                elif len(id) == 2:
                    i, j = id[0], id[1]
                    if    cashplayer[i]['startTime'] < cashplayer[j]['startTime']:
                          cashplayer[i]['endTime']   = cashplayer[j]['endTime']
                    else: cashplayer[i]['startTime'] = cashplayer[j]['startTime']
                    for idx,val in enumerate(cashplayer[j]['line']):
                        cashplayer[i]['line'][idx] += val
                    g = cashplayer.pop(j)
                    if pids[p]==heroes[0]:
                        self.cc[k][i]['ids'].append(hid)
                        self.cc[k][i]['ids'] += g['ids']
                elif len(id) == 0:
                    if cashplayer is None:
                        self.cc[k] = []
                    hp['endTime'] = hp['startTime']
                    if pids[p]==heroes[0]: hp['ids'].append(hid)
                    self.cc[k].append(hp)
        
        if doinsert:
            select_CC    = self.sql.query['select_CC'].replace('%s', self.sql.query['placeholder'])
            update_CC    = self.sql.query['update_CC'].replace('%s', self.sql.query['placeholder'])
            insert_CC    = self.sql.query['insert_CC'].replace('%s', self.sql.query['placeholder'])
            delete_CC    = self.sql.query['delete_CC'].replace('%s', self.sql.query['placeholder'])
            c = self.get_cursor()
            for k, cashplayer in self.cc.iteritems():
                for session in cashplayer:
                    hid = session['hid']
                    sc = self.sc.get(hid)
                    if sc is not None:
                        sid = sc['id']
                    else:
                        sid = None
                    lower = session['startTime'] - THRESHOLD
                    upper = session['endTime']   + THRESHOLD
                    row = [lower, upper] + list(k[:2])
                    c.execute(select_CC, row)
                    r = self.fetchallDict(c)
                    num = len(r)
                    d = [0]*num
                    for z in range(num):
                        d[z] = {}
                        d[z]['line'] = [int(r[z][s]) for s in CACHE_KEYS]
                        d[z]['id']   = r[z]['id']
                        d[z]['startTime'] = r[z]['sessionId']
                        d[z]['startTime'] = r[z]['startTime']
                        d[z]['endTime']   = r[z]['endTime']
                    if (num == 1):
                        start, end, id = r[0]['startTime'], r[0]['endTime'], r[0]['id']
                        if session['startTime'] < start:
                            start = session['startTime']
                        if session['endTime']   > end:
                            end = session['endTime']
                        row = [start, end] + session['line'] + [id]
                        c.execute(update_CC, row)
                    elif (num > 1):
                        start, end, merge, line = None, None, [], [0]*len(CACHE_KEYS)
                        for n in r: merge.append(n['id'])
                        merge.sort()
                        r = d
                        r.append(session)
                        for n in r:
                            if start:
                                if  start > n['startTime']: 
                                    start = n['startTime']
                            else:   start = n['startTime']
                            if end: 
                                if  end < n['endTime']: 
                                    end = n['endTime']
                            else:   end = n['endTime']
                            if not sid and n['sessionId']:
                                sid = n['id']
                            for idx in range(len(CACHE_KEYS)):
                                line[idx] += int(n['line'][idx])
                        row = [sid, start, end] + list(k[:2]) + line 
                        c.execute(insert_CC, row)
                        id = self.get_last_insert_id(c)
                        for m in merge:
                            c.execute(delete_CC, (m,))
                            self.commit()
                    elif (num == 0):
                        start               = session['startTime']
                        end                 = session['endTime']
                        row = [sid, start, end] + list(k[:2]) + session['line'] 
                        c.execute(insert_CC, row)
                        id = self.get_last_insert_id(c)              
            self.commit()
            
    def storeTourCache(self, hid, pids, startTime, tid, gametype, pdata, heroes, hero, doinsert = False):
        """Update cached tour sessions. If no record exists, do an insert"""   
        if gametype['type']=='tour' and pdata:
            for p in pdata:
                k = (tid
                    ,pids[p]
                    )
                pdata[p]['hands'] = 1
                line = [int(pdata[p][s]) for s in CACHE_KEYS]
                tourplayer = self.tc.get(k)
                # Add line to the old line in the tourcache.
                if tourplayer is not None:
                    for idx,val in enumerate(line):
                        tourplayer['line'][idx] += val
                    if pids[p]==heroes[0]:
                        tourplayer['ids'].append(hid)
                else:
                    self.tc[k] = {'startTime' : None,
                                          'endTime' : None,
                                              'hid' : hid,
                                              'ids' : []}
                    self.tc[k]['line'] = line
                    if pids[p]==heroes[0]:
                        self.tc[k]['ids'].append(hid)

                if not self.tc[k]['startTime'] or startTime < self.tc[k]['startTime']:
                    self.tc[k]['startTime']  = startTime
                if not self.tc[k]['endTime'] or startTime > self.tc[k]['endTime']:
                    self.tc[k]['endTime']    = startTime
                
        if doinsert:
            update_TC = self.sql.query['update_TC']
            update_TC = update_TC.replace('%s', self.sql.query['placeholder'])
            insert_TC = self.sql.query['insert_TC']
            insert_TC = insert_TC.replace('%s', self.sql.query['placeholder'])
            select_TC = self.sql.query['select_TC']
            select_TC = select_TC.replace('%s', self.sql.query['placeholder'])
            
            inserts = []
            c = self.get_cursor()
            for k, tc in self.tc.iteritems():
                hid = tc['hid']
                sc = self.sc.get(hid)
                if sc is not None:
                    sid = sc['id']
                    tc['sid'] = sid
                else:
                    sid = None
                if self.backend == self.SQLITE:
                    tc['startTime'] = datetime.strptime(tc['startTime'], '%Y-%m-%d %H:%M:%S')
                    tc['endTime']   = datetime.strptime(tc['endTime'], '%Y-%m-%d %H:%M:%S')
                else:
                    tc['startTime'] = tc['startTime'].replace(tzinfo=None)
                    tc['endTime']   = tc['endTime'].replace(tzinfo=None)
                c.execute(select_TC, k)
                result = c.fetchone()
                id, start, end = None, None, None
                if result:
                    id, start, end = result
                self.tc[k]['id'] = id
                update = not start or not end
                if (update or (tc['startTime']<start and tc['endTime']>end)):
                    q = update_TC.replace('<UPDATE>', 'startTime=%s, endTime=%s,')
                    row = [tc['startTime'], tc['endTime']] + tc['line'] + list(k[:2])
                elif tc['startTime']<start:
                    q = update_TC.replace('<UPDATE>', 'startTime=%s, ')
                    row = [tc['startTime']] + tc['line'] + list(k[:2])
                elif tc['endTime']>end:
                    q = update_TC.replace('<UPDATE>', 'endTime=%s, ')
                    row = [tc['endTime']] + tc['line'] + list(k[:2])
                else:
                    q = update_TC.replace('<UPDATE>', '')
                    row = tc['line'] + list(k[:2])
                
                num = c.execute(q, row)
                # Try to do the update first. Do insert it did not work
                if ((self.backend == self.PGSQL and c.statusmessage != "UPDATE 1")
                        or (self.backend == self.MYSQL_INNODB and num == 0)
                        or (self.backend == self.SQLITE and num.rowcount == 0)):
                    row = [sid, tc['startTime'], tc['endTime']] + list(k[:2]) + tc['line']
                    #append to the bulk inserts
                    inserts.append(row)
                
            if inserts:
                c.executemany(insert_TC, inserts)
            self.commit()
    
    def storeCardsCache(self, hid, pids, startTime, gid, ttid, gametype, siteId, pdata, sdata, heroes, tz_name, doinsert):
        """Update cached cards statistics. If update fails because no record exists, do an insert."""
        tourneyTypeId, gametypeId = None, None
        if gametype['type']=='ring':
            gametypeId = gid
        else:
            tourneyTypeId = ttid
        
        for p in pdata:
            if pids[p] in heroes:
                info = [hs for hs in sdata if hs[1]==pids[p]]
                for hs in info:
                    (pid, streetId, boardId, hiLo, rankId, start_cards) = (hs[1], hs[2], hs[3], hs[4], hs[5], pdata[p]['startCards'])
                    if streetId > 0: start_cards = 170
                    k =   (hid
                          ,gametypeId
                          ,tourneyTypeId
                          ,pids[p]
                          ,streetId
                          ,boardId
                          ,hiLo
                          ,start_cards
                          ,rankId
                          )
                    pdata[p]['hands'] = 1
                    line = [int(pdata[p][s]) for s in CACHE_KEYS]
                    self.dcbulk[k] = line

        if doinsert:
            update_cardscache = self.sql.query['update_cardscache']
            update_cardscache = update_cardscache.replace('%s', self.sql.query['placeholder'])
            insert_cardscache = self.sql.query['insert_cardscache']
            insert_cardscache = insert_cardscache.replace('%s', self.sql.query['placeholder'])
            select_cardscache_ring = self.sql.query['select_cardscache_ring']
            select_cardscache_ring = select_cardscache_ring.replace('%s', self.sql.query['placeholder'])
            select_cardscache_tour = self.sql.query['select_cardscache_tour']
            select_cardscache_tour = select_cardscache_tour.replace('%s', self.sql.query['placeholder'])
            
            select_WC     = self.sql.query['select_WC'].replace('%s', self.sql.query['placeholder'])
            select_MC     = self.sql.query['select_MC'].replace('%s', self.sql.query['placeholder'])
            insert_WC     = self.sql.query['insert_WC'].replace('%s', self.sql.query['placeholder'])
            insert_MC     = self.sql.query['insert_MC'].replace('%s', self.sql.query['placeholder'])
            
            dccache, inserts = {}, []
            for k, l in self.dcbulk.iteritems():
                sc = self.sc.get(k[0])
                if sc != None:
                    n = (sc['wid'], sc['mid'], k[1], k[2], k[3], k[4], k[5], k[6], k[7], k[8])         
                    startCards = dccache.get(n)
                    # Add line to the old line in the hudcache.
                    if startCards is not None:
                        for idx,val in enumerate(l):
                            dccache[n][idx] += val
                    else:
                        dccache[n] = l

            c = self.get_cursor()
            for k, item in dccache.iteritems():
                garbageWeekMonths = (k[0], k[1]) in self.wmnew or (k[0], k[1]) in self.wmold
                garbageTourneyTypes = k[2] in self.ttnew or k[2] in self.ttold
                if not garbageWeekMonths and not garbageTourneyTypes:
                    if k[2]:
                        q = select_cardscache_ring
                        row = list(k[:3]) + list(k[-6:])
                    else:
                        q = select_cardscache_tour
                        row = list(k[:2]) + list(k[-7:])
                    c.execute(q, row)
                    result = c.fetchone()
                    if result:
                        id = result[0]
                        update = item + [id]
                        c.execute(update_cardscache, update)                 
                    else:
                        insert = list(k) + item
                        inserts.append(insert)
                
            if inserts:
                c.executemany(insert_cardscache, inserts)
                self.commit()
            
    def storePositionsCache(self, hid, pids, startTime, gid, ttid, gametype, siteId, pdata, heroes, tz_name, doinsert):
        """Update cached position statistics. If update fails because no record exists, do an insert."""
        tourneyTypeId, gametypeId = None, None
        if gametype['type']=='ring':
            gametypeId = gid
        else:
            tourneyTypeId = ttid
            
        for p in pdata:
            if pids[p] in heroes:
                position = str(pdata[p]['position'])[0]
            else:
                position = 'N'
            k =   (hid
                  ,gametypeId
                  ,tourneyTypeId
                  ,pids[p]
                  ,len(pids)
                  ,position
                  )
            pdata[p]['hands'] = 1
            line = [int(pdata[p][s]) for s in CACHE_KEYS]
            self.pcbulk[k] = line
                
        if doinsert:
            update_positionscache = self.sql.query['update_positionscache']
            update_positionscache = update_positionscache.replace('%s', self.sql.query['placeholder'])
            insert_positionscache = self.sql.query['insert_positionscache']
            insert_positionscache = insert_positionscache.replace('%s', self.sql.query['placeholder'])
            
            select_positionscache_ring = self.sql.query['select_positionscache_ring']
            select_positionscache_ring = select_positionscache_ring.replace('%s', self.sql.query['placeholder'])
            select_positionscache_tour = self.sql.query['select_positionscache_tour']
            select_positionscache_tour = select_positionscache_tour.replace('%s', self.sql.query['placeholder'])
            
            select_WC     = self.sql.query['select_WC'].replace('%s', self.sql.query['placeholder'])
            select_MC     = self.sql.query['select_MC'].replace('%s', self.sql.query['placeholder'])
            insert_WC     = self.sql.query['insert_WC'].replace('%s', self.sql.query['placeholder'])
            insert_MC     = self.sql.query['insert_MC'].replace('%s', self.sql.query['placeholder'])
                
            pccache, inserts = {}, []
            for k, l in self.pcbulk.iteritems():
                sc = self.sc.get(k[0])
                if sc != None:
                    n = (sc['wid'], sc['mid'], k[1], k[2], k[3], k[4], k[5])         
                    positions = pccache.get(n)
                    # Add line to the old line in the hudcache.
                    if positions is not None:
                        for idx,val in enumerate(l):
                            pccache[n][idx] += val
                    else:
                        pccache[n] = l
            
            c = self.get_cursor()
            for k, item in pccache.iteritems():
                garbageWeekMonths = (k[0], k[1]) in self.wmnew or (k[0], k[1]) in self.wmold
                garbageTourneyTypes = k[2] in self.ttnew or k[2] in self.ttold
                if not garbageWeekMonths and not garbageTourneyTypes:
                    if k[2]:
                        q = select_positionscache_ring
                        row = list(k[:3]) + list(k[-3:])
                    else:
                        q = select_positionscache_tour
                        row = list(k[:2]) + list(k[-4:])
                    
                    c.execute(q, row)
                    result = c.fetchone()
                    if result:
                        id = result[0]
                        update = item + [id]
                        c.execute(update_positionscache, update)
                    else:
                        insert = list(k) + item
                        inserts.append(insert)
                
            if inserts:
                c.executemany(insert_positionscache, inserts)
                self.commit()
    
    def appendHandsSessionIds(self):
        for i in range(len(self.hbulk)):
            hid  = self.hids[i]
            tid = self.hbulk[i][2]
            sc = self.sc.get(hid)
            if sc is not None:
                self.hbulk[i][4] = sc['id']
                if tid: self.tbulk[tid] = sc['id']
                
    def get_id(self, file):
        q = self.sql.query['get_id']
        q = q.replace('%s', self.sql.query['placeholder'])
        c = self.get_cursor()
        c.execute(q, (file,))
        id = c.fetchone()
        if not id:
            return 0
        return id[0]

    def storeFile(self, fdata):
        q = self.sql.query['store_file']
        q = q.replace('%s', self.sql.query['placeholder'])
        c = self.get_cursor()
        c.execute(q, fdata)
        id = self.get_last_insert_id(c)
        return id
        
    def updateFile(self, fdata):
        q = self.sql.query['update_file']
        q = q.replace('%s', self.sql.query['placeholder'])
        c = self.get_cursor()
        c.execute(q, fdata)

    def getHeroIds(self, pids, sitename):
        #Grab playerIds using hero names in HUD_Config.xml
        try:
            # derive list of program owner's player ids
            hero = {}                               # name of program owner indexed by site id
            hero_ids = []
                                                         # make sure at least two values in list
                                                         # so that tuple generation creates doesn't use
                                                         # () or (1,) style
            for site in self.config.get_supported_sites():
                hero = self.config.supported_sites[site].screen_name
                for n, v in pids.iteritems():
                    if n == hero and sitename == site:
                        hero_ids.append(v)
                        
        except:
            err = traceback.extract_tb(sys.exc_info()[2])[-1]
            #print _("Error aquiring hero ids:"), str(sys.exc_value)
        return hero_ids
    
    def fetchallDict(self, cursor):
        data = cursor.fetchall()
        if not data: return []
        desc = cursor.description
        results = [0]*len(data)
        for i in range(len(data)):
            results[i] = {}
            for n in range(len(desc)):
                name = desc[n][0]
                results[i][name] = data[i][n]
        return results
    
    def nextHandId(self):
        c = self.get_cursor(True)
        c.execute("SELECT max(id) FROM Hands")
        id = c.fetchone()[0]
        if not id: id = 0
        id += self.hand_inc
        return id

    def isDuplicate(self, siteId, siteHandNo, heroSeat, publicDB):
        q = self.sql.query['isAlreadyInDB'].replace('%s', self.sql.query['placeholder'])
        if publicDB:
            key = (siteHandNo, siteId, heroSeat)
            q = q.replace('<heroSeat>', ' AND heroSeat=%s')
        else:
            key = (siteHandNo, siteId)
            q = q.replace('<heroSeat>', '')
        if key in self.siteHandNos:
            return True
        c = self.get_cursor()
        c.execute(q, key)
        result = c.fetchall()
        if len(result) > 0:
            return True
        self.siteHandNos.append(key)
        return False
    
    def getSqlPlayerIDs(self, pnames, siteid, hero):
        result = {}
        if(self.pcache == None):
            self.pcache = LambdaDict(lambda  key:self.insertPlayer(key[0], key[1], key[2]))

        for player in pnames:
            result[player] = self.pcache[(player,siteid,player==hero)]
            # NOTE: Using the LambdaDict does the same thing as:
            #if player in self.pcache:
            #    #print "DEBUG: cachehit"
            #    pass
            #else:
            #    self.pcache[player] = self.insertPlayer(player, siteid)
            #result[player] = self.pcache[player]

        return result
    
    def insertPlayer(self, name, site_id, hero):
        insert_player = "INSERT INTO Players (name, siteId, hero, chars) VALUES (%s, %s, %s, %s)"
        insert_player = insert_player.replace('%s', self.sql.query['placeholder'])
        _name = Charset.to_db_utf8(name)
        if re_char.match(_name[0]):
            char = '123'
        elif len(_name)==1 or re_char.match(_name[1]):
            char = _name[0] + '1'
        else:
            char = _name[:2]
        
        key = (_name, site_id, hero, char.upper())
        
        #NOTE/FIXME?: MySQL has ON DUPLICATE KEY UPDATE
        #Usage:
        #        INSERT INTO `tags` (`tag`, `count`)
        #         VALUES ($tag, 1)
        #           ON DUPLICATE KEY UPDATE `count`=`count`+1;


        #print "DEBUG: name: %s site: %s" %(name, site_id)
        result = None
        c = self.get_cursor()
        q = "SELECT id, name, hero FROM Players WHERE name=%s and siteid=%s"
        q = q.replace('%s', self.sql.query['placeholder'])
        result = self.insertOrUpdate('players', c, key, q, insert_player)
        return result
    
    def insertOrUpdate(self, type, cursor, key, select, insert):
        if type=='players':
            cursor.execute (select, key[:2])
        else:
            cursor.execute (select, key)
        tmp = cursor.fetchone()
        if (tmp == None):
            cursor.execute (insert, key)
            result = self.get_last_insert_id(cursor)
        else:
            result = tmp[0]
            if type=='players':
                if not tmp[2] and key[2]:
                    q = "UPDATE Players SET hero=%s WHERE name=%s and siteid=%s"
                    q = q.replace('%s', self.sql.query['placeholder'])
                    cursor.execute (q, (key[2], key[0], key[1]))
        return result
    
    def getSqlGameTypeId(self, siteid, game, printdata = False):
        if(self.gtcache == None):
            self.gtcache = LambdaDict(lambda  key:self.insertGameTypes(key[0], key[1]))
            
        self.gtprintdata = printdata
        hilo = Card.games[game['category']][2]
            
        gtinfo = (siteid, game['type'], game['category'], game['limitType'], game['currency'],
                  game['mix'], int(Decimal(game['sb'])*100), int(Decimal(game['bb'])*100), game['maxSeats'],
                  int(game['ante']*100), game['buyinType'], game['fast'], game['newToGame'], game['homeGame'])
        
        gtinsert = (siteid, game['currency'], game['type'], game['base'], game['category'], game['limitType'], hilo,
                    game['mix'], int(Decimal(game['sb'])*100), int(Decimal(game['bb'])*100),
                    int(Decimal(game['bb'])*100), int(Decimal(game['bb'])*200), game['maxSeats'], int(game['ante']*100),
                    game['buyinType'], game['fast'], game['newToGame'], game['homeGame'])
        
        result = self.gtcache[(gtinfo, gtinsert)]
        # NOTE: Using the LambdaDict does the same thing as:
        #if player in self.pcache:
        #    #print "DEBUG: cachehit"
        #    pass
        #else:
        #    self.pcache[player] = self.insertPlayer(player, siteid)
        #result[player] = self.pcache[player]

        return result

    def insertGameTypes(self, gtinfo, gtinsert):
        result = None
        c = self.get_cursor()
        q = self.sql.query['getGametypeNL']
        q = q.replace('%s', self.sql.query['placeholder'])
        c.execute(q, gtinfo)
        tmp = c.fetchone()
        if (tmp == None):
                
            if self.gtprintdata:
                print ("######## Gametype ##########")
                import pprint
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(gtinsert)
                print ("###### End Gametype ########")
                
            c.execute(self.sql.query['insertGameTypes'].replace('%s', self.sql.query['placeholder']), gtinsert)
            result = self.get_last_insert_id(c)
        else:
            result = tmp[0]
        return result
    
    def getTourneyInfo(self, siteName, tourneyNo):
        c = self.get_cursor()
        q = self.sql.query['getTourneyInfo'].replace('%s', self.sql.query['placeholder'])
        c.execute(q, (siteName, tourneyNo))
        columnNames=c.description

        names=[]
        for column in columnNames:
            names.append(column[0])

        data=c.fetchone()
        return (names,data)
    #end def getTourneyInfo

    def getTourneyTypesIds(self):
        c = self.connection.cursor()
        c.execute(self.sql.query['getTourneyTypesIds'])
        result = c.fetchall()
        return result
    #end def getTourneyTypesIds
    
    def getSqlTourneyTypeIDs(self, hand):
        #if(self.ttcache == None):
        #    self.ttcache = LambdaDict(lambda  key:self.insertTourneyType(key[0], key[1], key[2]))
            
        #tourneydata =   (hand.siteId, hand.buyinCurrency, hand.buyin, hand.fee, hand.gametype['category'],
        #                 hand.gametype['limitType'], hand.maxseats, hand.isSng, hand.isKO, hand.koBounty,
        #                 hand.isRebuy, hand.rebuyCost, hand.isAddOn, hand.addOnCost, hand.speed, hand.isShootout, hand.isMatrix)
        
        result = self.createOrUpdateTourneyType(hand) #self.ttcache[(hand.tourNo, hand.siteId, tourneydata)]

        return result
    
    def defaultTourneyTypeValue(self, value1, value2, field):
        if ((not value1) or 
           (field=='maxSeats' and value1>value2) or 
           ((field,value1)==('buyinCurrency','NA')) or 
           ((field,value1)==('stack','Regular')) or
           ((field,value1)==('speed','Normal')) or
           (field=='koBounty' and value1)
           ):
            return True
        return False
    
    def updateObjectValue(self, obj, dbVal, objVal, objField):
        if (objField=='koBounty' and objVal>dbVal and dbVal!=0):
            if objVal%dbVal==0:
                setattr(obj, objField, dbVal)
                koCounts = getattr(obj, 'koCounts')
                for pname, kos in koCounts.iteritems():
                    koCount = objVal/dbVal
                    obj.koCounts.update( {pname : [koCount] } )
        else:
            setattr(obj, objField, dbVal)
    
    def createOrUpdateTourneyType(self, obj):
        ttid, _ttid, updateDb = None, None, False
        cursor = self.get_cursor()
        q = self.sql.query['getTourneyTypeIdByTourneyNo'].replace('%s', self.sql.query['placeholder'])
        cursor.execute(q, (obj.tourNo, obj.siteId))
        result=cursor.fetchone()
        
        if result != None:
            columnNames=[desc[0] for desc in cursor.description]
            if self.backend == self.PGSQL:
                expectedValues = (('buyin', 'buyin'), ('fee', 'fee'), ('buyinCurrency', 'currency'),('isSng', 'sng'), ('maxseats', 'maxseats')
                             , ('isKO', 'knockout'), ('koBounty', 'kobounty'), ('isRebuy', 'rebuy'), ('rebuyCost', 'rebuycost')
                             , ('isAddOn', 'addon'), ('addOnCost','addoncost'), ('speed', 'speed'), ('isShootout', 'shootout')
                             , ('isMatrix', 'matrix'), ('isFast', 'fast'), ('stack', 'stack'), ('isStep', 'step'), ('stepNo', 'stepno')
                             , ('isChance', 'chance'), ('chanceCount', 'chancecount'), ('isMultiEntry', 'multientry'), ('isReEntry', 'reentry')
                             , ('isHomeGame', 'homegame'), ('isNewToGame', 'newtogame'), ('isFifty50', 'fifty50'), ('isTime', 'time')
                             , ('timeAmt', 'timeamt'), ('isSatellite', 'satellite'), ('isDoubleOrNothing', 'doubleornothing'), ('isCashOut', 'cashout')
                             , ('isOnDemand', 'ondemand'), ('isFlighted', 'flighted'), ('isGuarantee', 'guarantee'), ('guaranteeAmt', 'guaranteeamt'))
            else:
                expectedValues = (('buyin', 'buyin'), ('fee', 'fee'), ('buyinCurrency', 'currency'),('isSng', 'sng'), ('maxseats', 'maxSeats')
                             , ('isKO', 'knockout'), ('koBounty', 'koBounty'), ('isRebuy', 'rebuy'), ('rebuyCost', 'rebuyCost')
                             , ('isAddOn', 'addOn'), ('addOnCost','addOnCost'), ('speed', 'speed'), ('isShootout', 'shootout') 
                             , ('isMatrix', 'matrix'), ('isFast', 'fast'), ('stack', 'stack'), ('isStep', 'step'), ('stepNo', 'stepNo')
                             , ('isChance', 'chance'), ('chanceCount', 'chanceCount'), ('isMultiEntry', 'multiEntry'), ('isReEntry', 'reEntry')
                             , ('isHomeGame', 'homeGame'), ('isNewToGame', 'newToGame'), ('isFifty50', 'fifty50'), ('isTime', 'time')
                             , ('timeAmt', 'timeAmt'), ('isSatellite', 'satellite'), ('isDoubleOrNothing', 'doubleOrNothing'), ('isCashOut', 'cashOut')
                             , ('isOnDemand', 'onDemand'), ('isFlighted', 'flighted'), ('isGuarantee', 'guarantee'), ('guaranteeAmt', 'guaranteeAmt'))
            resultDict = dict(zip(columnNames, result))
            ttid = resultDict["id"]
            for ev in expectedValues:
                objField, dbField = ev
                objVal, dbVal = getattr(obj, objField), resultDict[dbField]
                if self.defaultTourneyTypeValue(objVal, dbVal, objField) and dbVal:#DB has this value but object doesnt, so update object
                    self.updateObjectValue(obj, dbVal, objVal, objField)
                elif self.defaultTourneyTypeValue(dbVal, objVal, objField) and objVal:#object has this value but DB doesnt, so update DB
                    updateDb=True
                    oldttid = ttid
        if not result or updateDb:
            if obj.gametype['mix']!='none':
                category = obj.gametype['mix']
            else:
                category = obj.gametype['category']
            row = (obj.siteId, obj.buyinCurrency, obj.buyin, obj.fee, category,
                   obj.gametype['limitType'], obj.maxseats, obj.isSng, obj.isKO, obj.koBounty,
                   obj.isRebuy, obj.rebuyCost, obj.isAddOn, obj.addOnCost, obj.speed, obj.isShootout, 
                   obj.isMatrix, obj.isFast, obj.stack, obj.isStep, obj.stepNo, obj.isChance, obj.chanceCount,
                   obj.isMultiEntry, obj.isReEntry, obj.isHomeGame, obj.isNewToGame, obj.isFifty50, obj.isTime,
                   obj.timeAmt, obj.isSatellite, obj.isDoubleOrNothing, obj.isCashOut, obj.isOnDemand, obj.isFlighted, 
                   obj.isGuarantee, obj.guaranteeAmt)
            cursor.execute (self.sql.query['getTourneyTypeId'].replace('%s', self.sql.query['placeholder']), row)
            tmp=cursor.fetchone()
            try:
                ttid = tmp[0]
            except TypeError: #this means we need to create a new entry
                if self.printdata:
                    print ("######## Tourneys ##########")
                    import pprint
                    pp = pprint.PrettyPrinter(indent=4)
                    pp.pprint(row)
                    print ("###### End Tourneys ########")
                cursor.execute (self.sql.query['insertTourneyType'].replace('%s', self.sql.query['placeholder']), row)
                ttid = self.get_last_insert_id(cursor)
            if updateDb:
                #print 'DEBUG createOrUpdateTourneyType:', 'old', oldttid, 'new', ttid, row
                q = self.sql.query['updateTourneyTypeId'].replace('%s', self.sql.query['placeholder'])
                cursor.execute(q, (ttid, obj.siteId, obj.tourNo))
                self.ttold.add(oldttid)
                self.ttnew.add(ttid)
        return ttid
    
    def cleanUpTourneyTypes(self):
        if self.ttold:
            if self.callHud and self.cacheSessions:
                tables = ('HudCache','CardsCache', 'PositionsCache')
            elif self.callHud:
                tables = ('HudCache',)
            elif self.cacheSessions:
                tables = ('CardsCache', 'PositionsCache')
            select = self.sql.query['selectTourneyWithTypeId'].replace('%s', self.sql.query['placeholder'])
            delete = self.sql.query['deleteTourneyTypeId'].replace('%s', self.sql.query['placeholder'])
            cursor = self.get_cursor()
            for ttid in self.ttold:
                for t in tables:
                    statement = 'clear%sTourneyType' % t
                    clear  = self.sql.query[statement].replace('%s', self.sql.query['placeholder'])
                    cursor.execute(clear, (ttid,))
                self.commit()
                cursor.execute(select, (ttid,))
                result=cursor.fetchone()
                if not result:
                    cursor.execute(delete, (ttid,))
                    self.commit()
            for ttid in self.ttnew:
                for t in tables:
                    statement = 'clear%sTourneyType' % t
                    clear  = self.sql.query[statement].replace('%s', self.sql.query['placeholder'])
                    cursor.execute(clear, (ttid,))
                self.commit()
            for t in tables:
                statement = 'fetchNew%sTourneyTypeIds' % t
                fetch  = self.sql.query[statement].replace('%s', self.sql.query['placeholder'])
                cursor.execute(fetch)
                for id in cursor.fetchall():
                    self.rebuild_cache(None, None, t, id[0])
                
                    
    def cleanUpWeeksMonths(self):
        if self.cacheSessions and self.wmold:
            selectWeekId = self.sql.query['selectSessionWithWeekId'].replace('%s', self.sql.query['placeholder'])
            selectMonthId = self.sql.query['selectSessionWithMonthId'].replace('%s', self.sql.query['placeholder'])
            deleteWeekId = self.sql.query['deleteWeekId'].replace('%s', self.sql.query['placeholder'])
            deleteMonthId = self.sql.query['deleteMonthId'].replace('%s', self.sql.query['placeholder'])
            cursor = self.get_cursor()
            weeks, months, wmids = set(), set(), set()
            for (wid, mid) in self.wmold:
                for t in ('CardsCache', 'PositionsCache'):
                    statement = 'clear%sWeeksMonths' % t
                    clear  = self.sql.query[statement].replace('%s', self.sql.query['placeholder'])
                    cursor.execute(clear, (wid, mid))
                self.commit()
                weeks.add(wid)
                months.add(mid)
            
            for wid in weeks:
                cursor.execute(selectWeekId, (wid,))
                result=cursor.fetchone()
                if not result:
                    cursor.execute(deleteWeekId, (wid,))
                    self.commit()
                    
            for mid in months:
                cursor.execute(selectMonthId, (mid,))
                result=cursor.fetchone()
                if not result:
                    cursor.execute(deleteMonthId, (mid,))
                    self.commit()
                    
            for (wid, mid) in self.wmnew:
                for t in ('CardsCache', 'PositionsCache'):
                    statement = 'clear%sWeeksMonths' % t
                    clear  = self.sql.query[statement].replace('%s', self.sql.query['placeholder'])
                    cursor.execute(clear, (wid, mid))
                self.commit()
                    
            if self.wmold:
                for t in ('CardsCache', 'PositionsCache'):
                    statement = 'fetchNew%sWeeksMonths' % t
                    fetch  = self.sql.query[statement].replace('%s', self.sql.query['placeholder'])
                    cursor.execute(fetch)
                    for (wid, mid) in cursor.fetchall():
                        wmids.add((wid, mid))         
                for wmid in wmids:
                    for t in ('CardsCache', 'PositionsCache'):
                        self.rebuild_cache(None, None, t, None, wmid)
            self.commit()
            
    def rebuild_caches(self):
        if self.callHud and self.cacheSessions:
            tables = ('HudCache','CardsCache', 'PositionsCache')
        elif self.cacheSessions:
            tables = ('CardsCache', 'PositionsCache')
        else:
            tables = ('HudCache',)
        for t in tables:
            self.rebuild_cache(None, None, t)
                
    def resetClean(self):
        self.ttold = set()
        self.ttnew = set()
        self.wmold = set()
        self.wmnew = set()
        
    def cleanRequired(self):
        if self.ttold or self.wmold:
            return True
        return False
    
    def getSqlTourneyIDs(self, hand):
        if(self.tcache == None):
            self.tcache = LambdaDict(lambda  key:self.insertTourney(key[0], key[1], key[2]))

        result = self.tcache[(hand.siteId, hand.tourNo, hand.tourneyTypeId)]

        return result
    
    def insertTourney(self, siteId, tourNo, tourneyTypeId):
        result = None
        c = self.get_cursor()
        q = self.sql.query['getTourneyByTourneyNo']
        q = q.replace('%s', self.sql.query['placeholder'])

        c.execute (q, (siteId, tourNo))

        tmp = c.fetchone()
        if (tmp == None): 
            c.execute (self.sql.query['insertTourney'].replace('%s', self.sql.query['placeholder']),
                        (tourneyTypeId, None, tourNo, None, None,
                         None, None, None, None, None, None, None, None, None))
            result = self.get_last_insert_id(c)
        else:
            result = tmp[0]
        return result
    
    def createOrUpdateTourney(self, summary):
        cursor = self.get_cursor()
        q = self.sql.query['getTourneyByTourneyNo'].replace('%s', self.sql.query['placeholder'])
        cursor.execute(q, (summary.siteId, summary.tourNo))

        columnNames=[desc[0] for desc in cursor.description]
        result=cursor.fetchone()

        if result != None:
            if self.backend == self.PGSQL:
                expectedValues = (('comment','comment'), ('tourneyName','tourneyname')
                        ,('totalRebuyCount','totalrebuycount'), ('totalAddOnCount','totaladdoncount')
                        ,('prizepool','prizepool'), ('startTime','starttime'), ('entries','entries')
                        ,('commentTs','commentts'), ('endTime','endtime'), ('added', 'added'), ('addedCurrency', 'addedcurrency'))
            else:
                expectedValues = (('comment','comment'), ('tourneyName','tourneyName')
                        ,('totalRebuyCount','totalRebuyCount'), ('totalAddOnCount','totalAddOnCount')
                        ,('prizepool','prizepool'), ('startTime','startTime'), ('entries','entries')
                        ,('commentTs','commentTs'), ('endTime','endTime'), ('added', 'added'), ('addedCurrency', 'addedCurrency'))
            updateDb=False
            resultDict = dict(zip(columnNames, result))

            tourneyId = resultDict["id"]
            for ev in expectedValues :
                if getattr(summary, ev[0])==None and resultDict[ev[1]]!=None:#DB has this value but object doesnt, so update object
                    setattr(summary, ev[0], resultDict[ev[1]])
                elif getattr(summary, ev[0])!=None and not resultDict[ev[1]]:#object has this value but DB doesnt, so update DB
                    updateDb=True
                #elif ev=="startTime":
                #    if (resultDict[ev] < summary.startTime):
                #        summary.startTime=resultDict[ev]
            if updateDb:
                q = self.sql.query['updateTourney'].replace('%s', self.sql.query['placeholder'])
                row = (summary.entries, summary.prizepool, summary.startTime, summary.endTime, summary.tourneyName,
                       summary.totalRebuyCount, summary.totalAddOnCount, summary.comment, summary.commentTs, 
                       summary.added, summary.addedCurrency, tourneyId
                      )
                cursor.execute(q, row)
        else:
            row = (summary.tourneyTypeId, None, summary.tourNo, summary.entries, summary.prizepool, summary.startTime,
                   summary.endTime, summary.tourneyName, summary.totalRebuyCount, summary.totalAddOnCount,
                   summary.comment, summary.commentTs, summary.added, summary.addedCurrency)
            if self.printdata:
                print ("######## Tourneys ##########")
                import pprint
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(row)
                print ("###### End Tourneys ########")
            cursor.execute (self.sql.query['insertTourney'].replace('%s', self.sql.query['placeholder']), row)
            tourneyId = self.get_last_insert_id(cursor)
        return tourneyId
    #end def createOrUpdateTourney

    def getTourneyPlayerInfo(self, siteName, tourneyNo, playerName):
        c = self.get_cursor()
        c.execute(self.sql.query['getTourneyPlayerInfo'], (siteName, tourneyNo, playerName))
        columnNames=c.description

        names=[]
        for column in columnNames:
            names.append(column[0])

        data=c.fetchone()
        return (names,data)
    #end def getTourneyPlayerInfo  
    
    def getSqlTourneysPlayersIDs(self, hand):
        result = {}
        if(self.tpcache == None):
            self.tpcache = LambdaDict(lambda  key:self.insertTourneysPlayers(key[0], key[1], key[2]))

        for player in hand.players:
            playerId = hand.dbid_pids[player[1]]
            result[player[1]] = self.tpcache[(playerId,hand.tourneyId,hand.entryId)]

        return result
    
    def insertTourneysPlayers(self, playerId, tourneyId, entryId):
        result = None
        c = self.get_cursor()
        q = self.sql.query['getTourneysPlayersByIds']
        q = q.replace('%s', self.sql.query['placeholder'])

        c.execute (q, (tourneyId, playerId, entryId))

        tmp = c.fetchone()
        if (tmp == None): #new player
            c.execute (self.sql.query['insertTourneysPlayer'].replace('%s',self.sql.query['placeholder'])
                      ,(tourneyId, playerId, entryId, None, None, None, None, None, None))
            #Get last id might be faster here.
            #c.execute ("SELECT id FROM Players WHERE name=%s", (name,))
            result = self.get_last_insert_id(c)
        else:
            result = tmp[0]
        return result
    
    def createOrUpdateTourneysPlayers(self, summary):
        tourneysPlayersIds, tplayers, inserts = {}, [], []
        cursor = self.get_cursor()
        cursor.execute (self.sql.query['getTourneysPlayersByTourney'].replace('%s', self.sql.query['placeholder']),
                            (summary.tourneyId,))
        result=cursor.fetchall()
        if result: tplayers += [i for i in result]
        for player, entries in summary.players.iteritems():
            playerId = summary.dbid_pids[player]
            for entryIdx in range(len(entries)):
                entryId = entries[entryIdx]
                if (playerId,entryId) in tplayers:
                    cursor.execute (self.sql.query['getTourneysPlayersByIds'].replace('%s', self.sql.query['placeholder']),
                                    (summary.tourneyId, playerId, entryId))
                    columnNames=[desc[0] for desc in cursor.description]
                    result=cursor.fetchone()
                    if self.backend == self.PGSQL:
                        expectedValues = (('rank','rank'), ('winnings', 'winnings')
                                ,('winningsCurrency','winningscurrency'), ('rebuyCount','rebuycount')
                                ,('addOnCount','addoncount'), ('koCount','kocount'))
                    else:
                        expectedValues = (('rank','rank'), ('winnings', 'winnings')
                                ,('winningsCurrency','winningsCurrency'), ('rebuyCount','rebuyCount')
                                ,('addOnCount','addOnCount'), ('koCount','koCount'))
                    updateDb=False
                    resultDict = dict(zip(columnNames, result))
                    tourneysPlayersIds[(player,entryId)]=result[0]
                    for ev in expectedValues :
                        summaryAttribute=ev[0]
                        if ev[0]!="winnings" and ev[0]!="winningsCurrency":
                            summaryAttribute+="s"
                        summaryDict = getattr(summary, summaryAttribute)
                        if summaryDict[player][entryIdx]==None and resultDict[ev[1]]!=None:#DB has this value but object doesnt, so update object 
                            summaryDict[player][entryIdx] = resultDict[ev[1]]
                            setattr(summary, summaryAttribute, summaryDict)
                        elif summaryDict[player][entryIdx]!=None and not resultDict[ev[1]]:#object has this value but DB doesnt, so update DB
                            updateDb=True
                    if updateDb:
                        q = self.sql.query['updateTourneysPlayer'].replace('%s', self.sql.query['placeholder'])
                        inputs = (summary.ranks[player][entryIdx],
                                  summary.winnings[player][entryIdx],
                                  summary.winningsCurrency[player][entryIdx],
                                  summary.rebuyCounts[player][entryIdx],
                                  summary.addOnCounts[player][entryIdx],
                                  summary.koCounts[player][entryIdx],
                                  tourneysPlayersIds[(player,entryId)]
                                 )
                        #print q
                        #pp = pprint.PrettyPrinter(indent=4)
                        #pp.pprint(inputs)
                        cursor.execute(q, inputs)
                else:
                    #print "all values: tourneyId",summary.tourneyId, "playerId",playerId, "rank",summary.ranks[player], "winnings",summary.winnings[player], "winCurr",summary.winningsCurrency[player], summary.rebuyCounts[player], summary.addOnCounts[player], summary.koCounts[player]
                    if summary.ranks[player][entryIdx]:
                        inserts.append((summary.tourneyId, playerId, entryId, int(summary.ranks[player][entryIdx]), 
                                        int(summary.winnings[player][entryIdx]), summary.winningsCurrency[player][entryIdx],
                                        summary.rebuyCounts[player][entryIdx], summary.addOnCounts[player][entryIdx], 
                                        summary.koCounts[player][entryIdx]))
                    else:
                        inserts.append((summary.tourneyId, playerId, entryId, None, None, None,
                                        summary.rebuyCounts[player][entryIdx], summary.addOnCounts[player][entryIdx],
                                        summary.koCounts[player][entryIdx]))
        if inserts:
            cursor.executemany(self.sql.query['insertTourneysPlayer'].replace('%s', self.sql.query['placeholder']),inserts)
            
    
#end class Database

if __name__=="__main__":
    c = Configuration.Config()
    sql = SQL.Sql(db_server = 'sqlite')

    db_connection = Database(c) # mysql fpdb holdem
#    db_connection = Database(c, 'fpdb-p', 'test') # mysql fpdb holdem
#    db_connection = Database(c, 'PTrackSv2', 'razz') # mysql razz
#    db_connection = Database(c, 'ptracks', 'razz') # postgres
    print "database connection object = ", db_connection.connection
    # db_connection.recreate_tables()
    db_connection.dropAllIndexes()
    db_connection.createAllIndexes()

    h = db_connection.get_last_hand()
    print "last hand = ", h

    hero = db_connection.get_player_id(c, 'PokerStars', 'nutOmatic')
    if hero:
        print "nutOmatic player_id", hero

    # example of displaying query plan in sqlite:
    if db_connection.backend == 4:
        print
        c = db_connection.get_cursor()
        c.execute('explain query plan '+sql.query['get_table_name'], (h, ))
        for row in c.fetchall():
            print "Query plan:", row
        print

    t0 = time()
    stat_dict = db_connection.get_stats_from_hand(h, "ring")
    t1 = time()
    for p in stat_dict.keys():
        print p, "  ", stat_dict[p]

    print _("cards ="), db_connection.get_cards(u'1')
    db_connection.close_connection

    print _("get_stats took: %4.3f seconds") % (t1-t0)

    print _("Press ENTER to continue.")
    sys.stdin.readline()

#Code borrowed from http://push.cx/2008/caching-dictionaries-in-python-vs-ruby
class LambdaDict(dict):
    def __init__(self, l):
        super(LambdaDict, self).__init__()
        self.l = l

    def __getitem__(self, key):
        if key in self:
            return self.get(key)
        else:
            self.__setitem__(key, self.l(key))
            return self.get(key)
