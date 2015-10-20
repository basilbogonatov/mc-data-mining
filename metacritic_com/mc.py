#!/usr/bin/python2
# -*- coding: utf-8 -*-
import urllib2
import re
from sys import stdout
from bs4 import BeautifulSoup
from codecs import getwriter
from json import dumps
from multiprocessing import Pool

def read_page(page_num):
    url = 'http://www.metacritic.com/browse/games/score/metascore/all/all/filtered?sort=desc&page=' + str(page_num)
    req = urllib2.Request(url)
    req.add_header('User-agent', 'Mozilla/5.0')
    return urllib2.urlopen(req).read()

def parse_page(page_num):
    return BeautifulSoup(read_page(page_num), 'html.parser')

def get_page_count():
    parsed_page = parse_page(0)
    return int(parsed_page.find(class_ = 'page last_page').a.string)

def extract_title_and_platform(parsed_game):
    extended_title = parsed_game[u'title']
    title, platform = extended_title.split('\n')[0:2]
    return title.strip(), platform.strip().strip('()')

def parse_single_game(game):
    parsed_game = { u'title'        : game.find(class_=re.compile('product.+title')).a.string,
                    u'link'         : game.find(class_=re.compile('product.+title')).a['href'],
                    u'metascore'    : game.find(class_='metascore_w').string,
                    u'release_date' : game.find(class_=re.compile('product.+date')).string,
                    u'userscore'    : game.find(class_=re.compile('product.+txt')).find(class_='data').string }

    parsed_game = dict(map(lambda (k,v): (k, v.strip()), parsed_game.iteritems()))
    parsed_game[u'title'], parsed_game[u'platform'] = extract_title_and_platform(parsed_game)

    return parsed_game

def parse_games(parsed_page):
    games = []
    for t in [' first', '', ' last']:
        games += parsed_page.find_all(class_='product_row game' + t)
    parsed_games = []
    for game in games:
        parsed_games.append(parse_single_game(game))

    return parsed_games

def serial_process(page_num):
    parsed_games = []
    try:
        parsed_games = parse_games(parse_page(page_num))
    except:
        pass
    return parsed_games

def merge_lists(parsed_games_list):
    games = []
    for game_list in parsed_games_list:
        games += game_list
    return games

def parallel_process(page_count):
    pool = Pool(processes=8) #that's enough
    parsed_games_list = pool.map(serial_process, range(page_count))

    #retry once for missing pages
    for page_num in range(len(parsed_games_list)):
        if len(parsed_games_list[page_num]) == 0:
            parsed_games_list[page_num] = serial_process(page_num)

    return merge_lists(parsed_games_list)

if __name__ == '__main__':
    page_count = 0
    try:
        page_count = get_page_count()
    except:
        pass
    else:
        sout = getwriter("utf8")(stdout)
        games = parallel_process(page_count)
        for game in games:
            sout.write(dumps(game, ensure_ascii=False) + "\n")