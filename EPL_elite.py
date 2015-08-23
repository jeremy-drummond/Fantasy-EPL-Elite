__author__ = 'Jeremy'
import re
import json
from decimal import Decimal
from time import sleep
import requests
import pypyodbc as pyodbc
from requests.exceptions import HTTPError, RequestException
from bs4 import BeautifulSoup


class IRequest(object):
    def get_data(self):
        raise NotImplementedError


class WebRequest(IRequest):
    def __init__(self, url):
        self._url = url

    def set_url(self, url):
        self._url = url

    def get_data(self, retry_limit=None):
        try:
            response = requests.get(self._url, timeout=30)
            if response.status_code != 200:
                raise HTTPError(response.status_code)
            return response.text
        except RequestException as error:
            if type(error) == HTTPError:
                raise error
            print("Connection timeout.")
            print("Timeout occurred on link: {0}".format(self._url))
            if retry_limit and retry_limit > 0:
                print("{0} retries remaining".format(retry_limit))
                print("retrying again in 5 minutes")
                sleep(300)
                print("retrying...")
                return self.get_data(retry_limit - 1)
            elif retry_limit == 0:
                raise error
            else:
                print("retrying in 5 minutes")
                sleep(300)
                print("retrying...")
                return self.get_data()

    def save_request(self, filename, request_data, encoding='UTF-8'):
        with open(filename, "w", encoding=encoding) as file:
            file.write(request_data)


class FileRequest(IRequest):
    def __init__(self, filename):
        self.filename = filename

    def get_data(self):
        with open(self.filename, 'r', encoding='UTF-8') as file:
            return file.read()


class LinkParser(object):
    def __init__(self):
        self.id_re_object = re.compile(r'[0-9]+')

    def extract_player_id(self, link):
        match = self.id_re_object.search(link)
        return match.group(0)

    def extract_gameweek(self, link):
        id_and_gameweek = re.findall(r'[0-9]+', link)
        gameweek = id_and_gameweek[1]
        return gameweek


class Scraper(object):
    def __init__(self, data):
        self.set_source_data(data)

    def set_source_data(self, data):
        self.parser = BeautifulSoup(data)


class StandingsScraper(Scraper):
    def scrape_standings_relative_links(self):
        css_selector = 'table.ismStandingsTable tr td a[href]'
        return [anchor_tag.attrs.get('href') for anchor_tag in self.parser.select(css_selector)]


class EntryScraper(Scraper):
    def __init__(self, page_html):
        super(EntryScraper, self).__init__(page_html)
        self.pitch_elements = []
        self.set_source_data(page_html)

    def set_source_data(self, page_html):
        super(EntryScraper, self).set_source_data(page_html)
        pitch_css_selector = 'div.ismPitchElement'
        self.pitch_elements = [anchor_tag.attrs.get('class') for anchor_tag in self.parser.select(pitch_css_selector)]
        manager_transfers_css_selector = 'dl.ismDefList.ismRHSDefList dd'
        self.rankings_points_transfers = self.parser.select(manager_transfers_css_selector)

    def scape_player_positions(self):
        POSITION_INDEX = -4
        return [int(item[POSITION_INDEX].strip(",")) for item in self.pitch_elements]

    def scrape_names(self):
        return [BeautifulSoup(str(item)).string.strip() for item in self.parser.select('div.ismPitchElement dt')]

    def scrape_player_ids(self):
        return [int(a.attrs.get('href')[1:]) for a in self.parser.select('span.JS_ISM_INFO a')]

    def scrape_vice_captain_index(self):
        VICE_CAPTAIN = -6
        return self._scrape_captain_squad_index(VICE_CAPTAIN)

    def scrape_captain_index(self):
        CAPTAIN = -10
        return self._scrape_captain_squad_index(CAPTAIN)

    def scrape_overall_points(self):
        OVERALL_POINTS = 0
        return BeautifulSoup(str(self.rankings_points_transfers[OVERALL_POINTS])).text.replace(',', '')

    def scrape_overall_rank(self):
        OVERALL_RANK = 1
        return BeautifulSoup(str(self.rankings_points_transfers[OVERALL_RANK])).text.replace(',', '')

    def scrape_total_players(self):
        TOTAL_PLAYERS = 2
        return BeautifulSoup(str(self.rankings_points_transfers[TOTAL_PLAYERS])).text.replace(',', '')

    def scrape_game_week_points(self):
        GAME_WEEK_POINTS = 3
        return BeautifulSoup(str(self.rankings_points_transfers[GAME_WEEK_POINTS])).text.replace(',', '')

    def scrape_total_transfers(self):
        TOTAL_TRANSFERS = 4
        return BeautifulSoup(str(self.rankings_points_transfers[TOTAL_TRANSFERS])).text.replace(',', '')

    def scrape_game_week_transfers(self):
        GAME_WEEK_TRANSFERS = 5
        return BeautifulSoup(str(self.rankings_points_transfers[GAME_WEEK_TRANSFERS])).text.replace(',', '')

    def scrape_wild_card_used(self):
        WILD_CARD = 6
        return "Not" in BeautifulSoup(str(self.rankings_points_transfers[WILD_CARD])).text

    def scrape_team_value(self):
        TEAM_VALUE = 7
        return BeautifulSoup(str(self.rankings_points_transfers[TEAM_VALUE])).text.strip("£m")

    def scrape_bank(self):
        BANK = 8
        return BeautifulSoup(str(self.rankings_points_transfers[BANK])).text.strip("£m")

    def scrape_team_name(self):
        css_selector = 'h2.ismSection3'
        return BeautifulSoup(str(self.parser.select(css_selector)[0])).text

    def scrape_manager_name(self):
        css_selector = "h1.ismSection2.ismWrapText"
        return BeautifulSoup(str(self.parser.select(css_selector)[0])).text

    def scrape_favorite_club(self):
        try:
            css_selector = 'img.ismRHSBadge'
            club_img_tag = self.parser.select(css_selector)[0]
            return club_img_tag.get('alt')
        except IndexError:
            return "None"

    def scrape_country(self):
        try:
            css_selector = 'img.ismRHSNat'
            club_img_tag = self.parser.select(css_selector)[0]
            return club_img_tag.get('alt')
        except IndexError:
            return "None"

    def _scrape_captain_squad_index(self, captain_code):
        """CAPTAIN = -10 & VICE_CAPTAIN = -6 """
        player_is_captain = [item[captain_code].strip(",") for item in self.pitch_elements]
        return player_is_captain.index("true")


class Manager(object):
    def __init__(self):
        self.id = 0
        self.name = ""
        self.club = ""
        self.country = ""
        self.team_name = ""
        self.finance = Finance()
        self.team = GameWeekTeam()


class Finance(object):
    def __init__(self):
        self.total_transfers = ""
        self.week_transfers = 0
        self.wildcard_available = False
        self.worth = 100
        self.bank = 0


class GameWeekTeam(object):
    def __init__(self):
        self.overall_points = 0
        self.overall_rank = 0
        self.game_week_points = 0
        self.players = []
        self.captain = None
        self.vice_captain = None

    def create_players(self, player_ids, player_names, player_positions):
        for i in range(len(player_ids)):
            p = Player()
            p.playerID = player_ids[i]
            p.name = player_names[i]
            p.position = player_positions[i]
            team_size = 11
            if i < team_size:
                p.started = True
            self.players.append(p)

    def set_captain(self, captain_index):
        self.captain = self.players[captain_index]

    def set_vice_captain(self, vice_captain_index):
        self.vice_captain = self.players[vice_captain_index]

class Player(object):
    def __init__(self):
        self.name = ""
        self.playerID = 0
        self.position = 0
        self.started = False

    def __str__(self):
        return self.name


class PlayerStats(object):
    def __init__(self, json_string):
        self.attributes_dict = json.loads(json_string)
        self._remove_unrequired_attributes()
        self._fix_atrribute_typing()

    def _remove_unrequired_attributes(self):
        unrequired_attributes = ["photo", "event_explain", "fixture_history", "season_history", "fixtures", "loans_in",
                                 "loans_out", "loaned_in", "loaned_out", "current_fixture", "next_fixture",
                                 "status", "code", "cost_change_start", "cost_change_event", "cost_change_start_fall",
                                 "cost_change_event_fall", "transfers_out_event", "transfers_in_event", "event_points",
                                 "ep_this", "ep_next", "special"]
        for attribute in unrequired_attributes:
            self.attributes_dict.pop(attribute)

    def _fix_atrribute_typing(self):
        incorrectly_typed_attributes = ["selected_by", "value_form", "value_season", "form", "selected_by_percent",
                                        "form", "points_per_game"]
        for attribute in incorrectly_typed_attributes:
            self.attributes_dict[attribute] = Decimal(self.attributes_dict[attribute])


class DbSaver(object):
    def __init__(self, game_week, connection_string, season=1415):
        # TODO refactor gameweek and season out of this class
        cnxn = pyodbc.connect(connection_string, autocommit=True)
        self.cursor = cnxn.cursor()
        self.sql_statements = ""
        self.game_week = game_week
        self.season = season

    def add_manager(self, manager):
        sql = ("INSERT INTO Manager (managerID, name, club, team_name, country, season) "
               "SELECT * FROM (SELECT {0} as managerID, '{1}' as name, '{2}' as club, '{3}' as team_name, "
               "'{4}' as country, {5} as season) AS tmp "
               "WHERE NOT EXISTS ( SELECT * from manager where managerID={0} and season={5} )"
               "; ").format(manager.id, manager.name.replace("'", "''"), manager.club.replace("'", "''"),
                            manager.team_name.replace("'", "''"), manager.country.replace("'", "''"), self.season)
        self.sql_statements += sql

    def add_finance(self, finance, manager_id):
        wildcard_available = 0
        if finance.wildcard_available:
            wildcard_available = 1
        sql = ("insert into Finance( game_week, total_transfers, week_transfers, wildcard_available, worth,"
               "bank, managerID, season) values({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7} )"
               "; ").format(self.game_week, finance.total_transfers, finance.week_transfers, wildcard_available,
                            finance.worth, finance.bank, manager_id, self.season)
        self.sql_statements += sql

    def add_player_stats(self, player_stats):
        field_names = []
        values = []

        for key, value in player_stats.attributes_dict.items():
            field_names.append(key)
            if value is False or value is None:
                value = 0
            if value is True:
                value = 1
            values.append(value)
        field_names += ["game_week", "season"]

        sql = "insert into Player({0}) values (".format(', '.join(field_names))
        for value in values:
            if type(value) == str:
                sql += "'{0}',".format(value.replace("'", "''"))
            else:
                sql += "{0}, ".format(value)
        sql += "{0}, {1})".format(self.game_week, self.season)

        sql += "ON DUPLICATE KEY UPDATE "
        LAST_FIELD = -1
        for field in field_names[:LAST_FIELD]:
            sql += "{0}=VALUES({0}),".format(field)
        sql += "{0}=VALUES({0});".format(field_names[LAST_FIELD])
        self.sql_statements += sql.replace('""', 'null')

    def add_game_week_team(self, team, manager_id):
        game_week_team_id = str(manager_id) + "-" + str(self.game_week)
        sql = ("insert into GameWeekTeam (gameWeekTeamID, game_week, overall_points, overall_rank, game_week_points,"
               "captainID, vice_captainID, managerID, season) "
               "values('{0}', {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})"
               "; ").format(game_week_team_id, self.game_week, team.overall_points, team.overall_rank,
                            team.game_week_points, team.captain.playerID, team.vice_captain.playerID, manager_id,
                            self.season)

        self.sql_statements += sql

    def add_player(self, player, manager_id):
        game_week_team_id = str(manager_id) + "-" + str(self.game_week)
        started = 0
        if player.started:
            started = 1
        sql = ("insert into Position (id, gameWeekTeamID, started ) "
               "values({0}, '{1}', {2}); ").format(player.playerID, game_week_team_id, started)
        self.sql_statements += sql

    def commit(self):
        if self.sql_statements:
            with open(r"Execution_log.sql", "a", encoding='UTF-8') as my_file:
                my_file.write(self.sql_statements)
            self.sql_statements = self.sql_statements.replace("True", "1")
            self.sql_statements = self.sql_statements.replace("False", "0")
            try:
                self.cursor.execute(self.sql_statements)
            except pyodbc.Error as e:
                print(e)
                with open(r"Exceptions.txt", "a", encoding='UTF-8') as my_file:
                    except_log_msg = ("\n\n\n------------------Error---------------------\n\n"
                                      + str(e) + "\n\n")
                    for s in self.sql_statements.split(";"):
                        except_log_msg += s + ";\n"
                    my_file.write(except_log_msg)
            self.sql_statements = ""


class FantasyEPLController(object):
    def __init__(self, *connection_strings, league_id=313, season=1415):
        self.league_id = league_id
        self.season = season
        self.web = WebRequest('http://fantasy.premierleague.com/my-leagues/{0}/standings/?ls-page=1'.format(league_id))
        self.link_parser = LinkParser()
        self.entry_scraper = EntryScraper("")
        self.standings_scraper = StandingsScraper("")
        self.game_week = self._get_current_game_week()
        self._initialise_storage_handlers(*connection_strings)

    def download_player_stats(self):
        player_id = 1
        player_stats_remaining = True
        while player_stats_remaining:
            try:
                self.web.set_url("http://fantasy.premierleague.com/web/api/elements/{0}/".format(player_id))
                p = PlayerStats(self.web.get_data())
                for handler in self.storage_handlers:
                    handler.add_player_stats(p)
                player_id += 1
                print("added {0} : {1}".format(p.attributes_dict['id'], p.attributes_dict['second_name']))
            except HTTPError:
                print("Completed")
                player_stats_remaining = False
                for handler in self.storage_handlers:
                    handler.commit()

    def download_manager_stats(self, starting_rank=1, finishing_rank=10000):
        PAGE_SIZE = 50
        standings_page_total = int(float(finishing_rank / PAGE_SIZE))
        standings_page_index = int(float(starting_rank / PAGE_SIZE)) + 1
        total_records = ((standings_page_total + 1) - standings_page_index) * PAGE_SIZE
        print("Processing pages {0} through {1} for a total of {2} records.".format(standings_page_index,
                                                                                    standings_page_total,
                                                                                    total_records))
        while standings_page_index <= standings_page_total:
            # sleep(1)
            url = 'http://fantasy.premierleague.com/my-leagues/{0}/standings/?ls-page={1}'.format(self.league_id,
                                                                                                  standings_page_index)
            print("Processing page {0}.".format(standings_page_index))
            self.web.set_url(url)
            self.standings_scraper.set_source_data(self.web.get_data())
            links = self.standings_scraper.scrape_standings_relative_links()
            self._process_standings_page(links)
            print("Page {0} added.".format(standings_page_index))
            standings_page_index += 1

    def _get_current_game_week(self):
            self.web.set_url(('http://fantasy.premierleague.com/my-leagues/{0}/standings'
                              '/?ls-page=1').format(self.league_id))
            standings_html = self.web.get_data()
            self.standings_scraper.set_source_data(standings_html)
            FIRST_LINK_INDEX = 0
            first_link = self.standings_scraper.scrape_standings_relative_links()[FIRST_LINK_INDEX]
            self.entry_scraper.set_source_data(first_link)
            return self.link_parser.extract_gameweek(first_link)

    def _initialise_storage_handlers(self, *connection_strings):
        self.storage_handlers = []
        if connection_strings:
            for connection_string in connection_strings:
                handler = DbSaver(self.game_week, connection_string, self.season)
                self.storage_handlers.append(handler)
        else:
            default_connection_string = ("DRIVER={MySQL ODBC 5.3 Unicode Driver};SERVER=localhost;DATABASE=epl_15_16;"
                                         "USER=root;PASSWORD=admin;OPTION=67108864;")
            default_db = DbSaver(self.game_week, default_connection_string, self.season)
            self.storage_handlers.append(default_db)

    def _process_standings_page(self, links):
        for link in links:
            # sleep(1)
            url = 'http://fantasy.premierleague.com' + link
            self.web.set_url(url)
            self.entry_scraper.set_source_data(self.web.get_data())
            man = self._create_manager(link)
            team = self._create_game_week_team()

            for handler in self.storage_handlers:
                handler.add_manager(man)
                handler.add_game_week_team(team, man.id)
                for player in team.players:
                    handler.add_player(player, man.id)

        for handler in self.storage_handlers:
            handler.commit()

    def _create_manager(self, link):
        man = Manager()
        man.club = self.entry_scraper.scrape_favorite_club()
        man.country = self.entry_scraper.scrape_country()
        man.name = self.entry_scraper.scrape_manager_name()
        man.team_name = self.entry_scraper.scrape_team_name()
        man.id = self.link_parser.extract_player_id(link)
        return man

    def _create_game_week_team(self):
        team = GameWeekTeam()
        team.game_week_points = self.entry_scraper.scrape_game_week_points()
        team.overall_points = self.entry_scraper.scrape_overall_points()
        team.overall_rank = self.entry_scraper.scrape_overall_rank()
        playerIDs = self.entry_scraper.scrape_player_ids()
        playerNames = self.entry_scraper.scrape_names()
        positions = self.entry_scraper.scape_player_positions()
        team.create_players(playerIDs, playerNames, positions)
        team.set_captain(self.entry_scraper.scrape_captain_index())
        team.set_vice_captain(self.entry_scraper.scrape_vice_captain_index())
        return team


if __name__ == '__main__':
    epl = FantasyEPLController()
    epl.download_player_stats()
    epl.download_manager_stats(1, 10000)

    # TODO: - better logging, docstrings, refactoring

	#Connections
    # azure_connection_string = ("Driver={SQL Server Native Client 11.0};
    #                            "Server=tcp:txdy2atl0i.database.windows.net,1433;"
    #                            "Database=EPL;Uid=********@txdy2atl0i;Pwd=*******;Encrypt=yes;"
    #                            "Connection Timeout=30;")

    # my_sql_connection_string = ("DRIVER={MySQL ODBC 5.3 Unicode Driver};SERVER=localhost;DATABASE=epl;"
    #                            "USER=root;PASSWORD=admin;OPTION=67108864;")