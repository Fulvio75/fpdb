#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Copyright 2010-2011 Steffen Schaumburg
#This program is free software: you can redistribute it and/or modify
#it under the terms of the GNU Affero General Public License as published by
#the Free Software Foundation, version 3 of the License.
#
#This program is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU Affero General Public License
#along with this program. If not, see <http://www.gnu.org/licenses/>.
#In the "official" distribution you can find the license in agpl-3.0.txt.

import L10n
_ = L10n.get_translation()

import pygtk
pygtk.require('2.0')
import gtk
from time import time, strftime

import Charset
import TourneyFilters

colalias,colshow,colheading,coltooltip,colxalign,colformat,coltype = 0,1,2,3,4,5,6

class GuiTourneyPlayerStats:
    def __init__(self, config, db, sql, mainwin, debug=True):
        self.conf = config
        self.db = db
        self.cursor = self.db.cursor
        self.sql = sql
        self.main_window = mainwin
        self.debug = debug
        
        self.liststore = []   # gtk.ListStore[]         stores the contents of the grids
        self.listcols = []    # gtk.TreeViewColumn[][]  stores the columns in the grids
        
        filters_display = { "Heroes"    : True,
                            "Sites"     : True,
                            #"Games"     : True,
                            #"Limits"    : True,
                            #"LimitSep"  : True,
                            #"LimitType" : True,
                            "Type"      : True,
                            "Seats"     : True,
                            #"SeatSep"   : True,
                            "Dates"     : True,
                            #"Groups"    : True,
                            #"GroupsAll" : True,
                            #"Button1"   : True,
                            "Button2"   : True}
        
        self.stats_frame = None
        self.stats_vbox = None
        self.detailFilters = []   # the data used to enhance the sql select

        self.main_hbox = gtk.HPaned()

        self.filters = TourneyFilters.TourneyFilters(self.db, self.conf, self.sql, display = filters_display)
        #self.filters.registerButton1Name(_("_Filters"))
        #self.filters.registerButton1Callback(self.showDetailFilter)
        self.filters.registerButton2Name(_("_Refresh Stats"))
        self.filters.registerButton2Callback(self.refreshStats)
        
        # ToDo: store in config
        # ToDo: create popup to adjust column config
        # columns to display, keys match column name returned by sql, values in tuple are:
        #                   is column displayed, column heading, column tooltip, xalignment, formatting, celltype
        self.columns = [ ["siteName",       True,  _("Site"),       _("Site"),          0.0, "%s", "str"]
                       #,["tourney",        False, _("Tourney"),    _("Site"),          0.0, "%s", "str"]   # true not allowed for this line
                       , ["category",       True,  _("Cat."),       _("Game Type"),     0.0, "%s", "str"]
                       , ["limitType",      True,  _("Limit"),      _("Limit Type"),    0.0, "%s", "str"]
                       , ["currency",       True,  _("Curr."),      _("Currency"),      0.0, "%s", "str"]
                       , ["totalBuyIn",     True,  _("BuyIn"),      _("Total buy in"),  1.0, "%3.2f", "str"]
                       , ["fee",            True,  _("Fee"),        _("Rake"),          1.0, "%3.2f", "str"]
                       , ["maxseats",       True,  _("Seats"),      _("Max seats"),     1.0, "%1.0f", "str"]
                       , ["kobounty",       True,  _("KoBounty"),   _("KO bounty"),     1.0, "%1.0f", "str"]
                       , ["playerName",     False, _("Name"),       _("Player name"),   0.0, "%s", "str"]   # true not allowed for this line (set in code)
                       , ["tourneyCount",   True,  _("#"),          _("# of tourneys"), 1.0, "%1.0f", "str"]
                       , ["itm",            True,  _("ITM%"),       _("% in the money"),1.0, "%3.2f", "str"]
                       , ["_1st",           False, _("1st"),        _("# of 1st place"),1.0, "%1.0f", "str"]
                       , ["_2nd",           True,  _("2nd"),        _("# of 2nd place"),1.0, "%1.0f", "str"]
                       , ["_3rd",           True,  _("3rd"),        _("# of 3rd place"),1.0, "%1.0f", "str"]
                       , ["unknownRank",    True,  _("Rank?"),      _("To understand."),1.0, "%1.0f", "str"]
                       , ["spent",          True,  _("Spent"),      _("Invested money"),1.0, "%3.2f", "str"]
                       , ["won",            True,  _("Winnings"),   _("Total winnings"),1.0, "%3.2f", "str"]
                       , ["netWinnings",    True,  _("NetWin"),     _("Net winnings"),  1.0, "%3.2f", "str"]
                       , ["roi",            True,  _("ROI%"),       _("Return on investment %"),    1.0, "%3.2f", "str"]
                       , ["profitPerTourney", True,_("$/Tour"),     _("Profit per tourney"),    1.0, "%3.2f", "str"]]
                       
        self.stats_frame = gtk.Frame()
        self.stats_frame.show()

        self.stats_vbox = gtk.VPaned()
        self.stats_vbox.show()
        self.stats_frame.add(self.stats_vbox)
        # self.fillStatsFrame(self.stats_vbox)

        #self.main_hbox.pack_start(self.filters.get_vbox())
        #self.main_hbox.pack_start(self.stats_frame, expand=True, fill=True)
        self.main_hbox.pack1(self.filters.get_vbox())
        self.main_hbox.pack2(self.stats_frame)
        self.main_hbox.show()
    #end def __init__

    def addGrid(self, vbox, queryName, numTourneys, tourneyTypes, playerids, sitenos, seats):
        #print "start of addGrid query", query
        #print "start of addGrid. numTourneys:",numTourneys,"tourneyTypes:", tourneyTypes, "playerids:",playerids
        counter = 0
        row = 0
        sqlrow = 0
        grid=numTourneys #TODO: should this be numTourneyTypes?
        
        query = self.sql.query[queryName]
        #query = self.refineQuery(query, numTourneys, tourneyTypes, sitenos, seats)
        #print "DEBUG:\n%s" % query

        #Filter on dates
        start_date, end_date = self.filters.getDates()
        
        namedSqlParameters = {
            'players': playerids, 
            'sites': sitenos, 
            'startdate': start_date, 
            'enddate': end_date }

        self.cursor.execute(query, namedSqlParameters)
        result = self.cursor.fetchall()
        #print "result of the big query in addGrid:",result
        colnames = [desc[0] for desc in self.cursor.description]

        # pre-fetch some constant values:
        #self.cols_to_show = [x for x in self.columns if x[colshow]]
        #htourneytypeid_idx = colnames.index('tourneyTypeId')
        self.cols_to_show = self.columns #TODO do i need above 2 lines?
        
        assert len(self.liststore) == grid, "len(self.liststore)="+str(len(self.liststore))+" grid-1="+str(grid)
        self.liststore.append( gtk.ListStore(*([str] * len(self.cols_to_show))) )
        view = gtk.TreeView(model=self.liststore[grid])
        view.set_grid_lines(gtk.TREE_VIEW_GRID_LINES_BOTH)
        #vbox.pack_start(view, expand=False, padding=3)
        vbox.add(view)
        textcell = gtk.CellRendererText()
        textcell50 = gtk.CellRendererText()
        textcell50.set_property('xalign', 0.5)
        numcell = gtk.CellRendererText()
        numcell.set_property('xalign', 1.0)
        assert len(self.listcols) == grid
        self.listcols.append( [] )

        # Create header row   eg column: ("game",     True, "Game",     0.0, "%s")
        for col, column in enumerate(self.cols_to_show):
            if column[colalias] == 'game' and holecards:
                s = [x for x in self.columns if x[colalias] == 'hand'][0][colheading]
            else:
                s = column[colheading]

            tooltips = gtk.Tooltips()
            currentTreeViewColumn = gtk.TreeViewColumn()
            currentColumnHeader = gtk.Label(s)
            currentColumnHeader.show()
            currentTreeViewColumn.set_widget(currentColumnHeader)
            tooltips.set_tip(currentColumnHeader, column[coltooltip])
            self.listcols[grid].append(currentTreeViewColumn)

            view.append_column(self.listcols[grid][col])
            if column[colformat] == '%s':
                if column[colxalign] == 0.0:
                    self.listcols[grid][col].pack_start(textcell, expand=True)
                    self.listcols[grid][col].add_attribute(textcell, 'text', col)
                    cellrend = textcell
                else:
                    self.listcols[grid][col].pack_start(textcell50, expand=True)
                    self.listcols[grid][col].add_attribute(textcell50, 'text', col)
                    cellrend = textcell50
                self.listcols[grid][col].set_expand(True)
            else:
                self.listcols[grid][col].pack_start(numcell, expand=True)
                self.listcols[grid][col].add_attribute(numcell, 'text', col)
                self.listcols[grid][col].set_expand(True)
                cellrend = numcell
                #self.listcols[grid][col].set_alignment(column[colxalign]) # no effect?
            self.listcols[grid][col].set_clickable(True)
            self.listcols[grid][col].connect("clicked", self.sortCols, (col,grid))
            if col == 0:
                self.listcols[grid][col].set_sort_order(gtk.SORT_DESCENDING)
                self.listcols[grid][col].set_sort_indicator(True)
            if column[coltype] == 'cash':
                self.listcols[grid][col].set_cell_data_func(numcell, self.ledger_style_render_func)
            else:
                self.listcols[grid][col].set_cell_data_func(cellrend, self.reset_style_render_func)

        rows = len(result) # +1 for title row

        speedIndexColumn = colnames.index('speed');
        siteNameIndexColumn = colnames.index('siteName');

        while sqlrow < rows:
            treerow = []
            currentSqlRow = result[sqlrow]; 
            for column in self.cols_to_show:
                if column[colalias] in colnames:
                    value = currentSqlRow[colnames.index(column[colalias])]
                
                    if column[colalias] == 'siteName':
                        if currentSqlRow[speedIndexColumn] != 'Normal':
                            if (currentSqlRow[speedIndexColumn] == 'Hyper' 
                                and currentSqlRow[siteNameIndexColumn] ==
                                'Full Tilt Poker'):
                                value = value + ' ' + 'Super Turbo'
                            else:
                                value = value + ' ' + currentSqlRow[speedIndexColumn]
                    if value != None:
                        treerow.append(column[colformat] % value)
                    else:
                        treerow.append(' ')
                else:
                    treerow.append('Column alias not found in query')
            #print "addGrid, just before end of big for. grid:",grid,"treerow:",treerow
            iter = self.liststore[grid].append(treerow)
            sqlrow += 1
            row += 1
        vbox.show_all()
    #end def addGrid

    def createStatsTable(self, vbox, tourneyTypes, playerids, sitenos, seats):
        startTime = time()
        show_detail = True

        # Scrolled window for summary table
        swin = gtk.ScrolledWindow(hadjustment=None, vadjustment=None)
        swin.set_policy(gtk.POLICY_AUTOMATIC, gtk.POLICY_AUTOMATIC)
        swin.show()
        vbox.pack1(swin)

        numTourneys = self.filters.getNumTourneys()
        self.addGrid(swin, 'tourneyPlayerDetailedStats', numTourneys, tourneyTypes, playerids, sitenos, seats)

        print _("Stats page displayed in %4.2f seconds") % (time() - startTime)
    #end def createStatsTable

    def fillStatsFrame(self, vbox):
        tourneyTypes = self.filters.getTourneyTypes()
        #tourneys = self.tourneys.getTourneys()
        sites = self.filters.getSites()
        heroes = self.filters.getHeroes()
        siteids = self.filters.getSiteIds()
        seats  = self.filters.getSeats()
        dates = self.filters.getDates()
        sitenos = []
        playerids = []

        # Which sites are selected?
        for site in sites:
            if sites[site] == True:
                sitenos.append(siteids[site])
                _hname = Charset.to_utf8(heroes[site])
                result = self.db.get_player_id(self.conf, site, _hname)
                if result is not None:
                    playerids.append(int(result))

        if not sitenos:
            #Should probably pop up here.
            print _("No sites selected - defaulting to PokerStars")
            sitenos = [2]
        if not playerids:
            print _("No player ids found")
            return
        
        self.createStatsTable(vbox, tourneyTypes, playerids, sitenos, seats)
    #end def fillStatsFrame

    def get_vbox(self):
        """returns the vbox of this thread"""
        return self.main_hbox
    #end def get_vbox
    
    def refineQuery(self, query, numTourneys, tourneyTypes, sitenos, seats):
        having = ''
        
        
        #print "refinequery, nametest after initial creation:",nametest
        pname = "p.name"
        # set flag in self.columns to not show player name column
        #[x for x in self.columns if x[0] == 'pname'][0][1] = False #TODO: fix and reactivate
            
        query = query.replace("<nametest>", nametest)
        query = query.replace("<playerName>", pname)
        query = query.replace("<havingclause>", having)

        sitetest = ""
        q = []
        for m in self.filters.display.items():
            if m[0] == 'Sites' and m[1]:
                for n in sitenos:
                        q.append(n)
                if len(q) > 0:
                    sitetest = str(tuple(q))
                    sitetest = sitetest.replace("L", "")
                    sitetest = sitetest.replace(",)",")")
                    sitetest = sitetest.replace("u'","'")
                    sitetest = "and tt.siteId in %s" % sitetest#[1:-1]
                else:
                    sitetest = "and tt.siteId IS NULL"
        #print "refinequery, sitetest before its use for replacement:",sitetest
        query = query.replace("<sitetest>", sitetest)
        
        if seats:
            query = query.replace('<seats_test>', 'between ' + str(seats['from']) + ' and ' + str(seats['to']))
            if 'show' in seats and seats['show']:
                query = query.replace('<groupbyseats>', ',h.seats')
                query = query.replace('<orderbyseats>', ',h.seats')
            else:
                query = query.replace('<groupbyseats>', '')
                query = query.replace('<orderbyseats>', '')
        else:
            query = query.replace('<seats_test>', 'between 0 and 100')
            query = query.replace('<groupbyseats>', '')
            query = query.replace('<orderbyseats>', '')

        #bbtest = self.filters.get_limits_where_clause(limits)

        #query = query.replace("<gtbigBlind_test>", bbtest)

        #query = query.replace("<orderbyhgametypeId>", "")
        
        # process self.detailFilters (a list of tuples)
        flagtest = ''
        #self.detailFilters = [('h.seats', 5, 6)]   # for debug
        if self.detailFilters:
            for f in self.detailFilters:
                if len(f) == 3:
                    # X between Y and Z
                    flagtest += ' and %s between %s and %s ' % (f[0], str(f[1]), str(f[2]))
        query = query.replace("<flagtest>", flagtest)

        # allow for differences in sql cast() function:
        if self.db.backend == self.db.MYSQL_INNODB:
            query = query.replace("<signed>", 'signed ')
        else:
            query = query.replace("<signed>", '')

        # Filter on dates
        start_date, end_date = self.filters.getDates()
        query = query.replace("<startdate_test>", start_date)
        query = query.replace("<enddate_test>", end_date)

        return(query)
    #end def refineQuery

    def refreshStats(self, widget, data):
        self.last_pos = self.stats_vbox.get_position()
        try: self.stats_vbox.destroy()
        except AttributeError: pass
        self.liststore = []
        self.listcols = []
        #self.stats_vbox = gtk.VBox(False, 0)
        self.stats_vbox = gtk.VPaned()
        self.stats_vbox.show()
        self.stats_frame.add(self.stats_vbox)
        self.fillStatsFrame(self.stats_vbox)
        if self.last_pos > 0:
            self.stats_vbox.set_position(self.last_pos)
    #end def refreshStats
    
    def reset_style_render_func(self, treeviewcolumn, cell, model, iter):
        cell.set_property('foreground', None)
    #end def reset_style_render_func

    def sortCols(self, col, nums):
        #This doesn't actually work yet - clicking heading in top section sorts bottom section :-(
        (n, grid) = nums
        if not col.get_sort_indicator() or col.get_sort_order() == gtk.SORT_ASCENDING:
            col.set_sort_order(gtk.SORT_DESCENDING)
        else:
            col.set_sort_order(gtk.SORT_ASCENDING)
        self.liststore[grid].set_sort_column_id(n, col.get_sort_order())
        self.liststore[grid].set_sort_func(n, self.sortnums, (n,grid))
        for i in xrange(len(self.listcols[grid])):
            self.listcols[grid][i].set_sort_indicator(False)
        self.listcols[grid][n].set_sort_indicator(True)
        # use this   listcols[col].set_sort_indicator(True)
        # to turn indicator off for other cols
    #end def sortCols
#end class GuiTourneyPlayerStats
