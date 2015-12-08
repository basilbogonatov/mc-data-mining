#!/usr/bin/python2
# -*- coding: utf-8 -*-
import urllib2
import re
from sys import stdout
from bs4 import BeautifulSoup
from codecs import getwriter
from json import dumps
from multiprocessing.pool import ThreadPool

def strip_dict(dict_):
    for k in dict_.keys():
        if dict_[k] != None:
            dict_[k] = dict_[k].strip()
    return dict_

def get_page_soup(url):
    req = urllib2.Request(url)
    req.add_header('User-agent', 'Mozilla/5.0')
    return BeautifulSoup(urllib2.urlopen(req).read(), 'html.parser')

def parse_games_table_page(page_num):
    try:
        return get_page_soup('http://www.metacritic.com/browse/games/score/metascore/all/all/filtered?sort=desc&page=' + str(page_num))
    except:
        return None

def get_page_count():
    page = parse_games_table_page(0)
    try:
        return int(page.find(class_ = 'page last_page').a.string)
    except:
        return 0

def extract_title_and_platform(parsed_game):
    extended_title = parsed_game[u'title']
    title, platform = extended_title.split('\n')[0:2]
    return title.strip(), platform.strip().strip('()')

def parse_single_game_from_table(game):
    parsed_game = { u'title'        : game.find(class_=re.compile('product.+title')).a.string,
                    u'link'         : game.find(class_=re.compile('product.+title')).a['href'],
                    u'metascore'    : game.find(class_='metascore_w').string,
                    u'release_date' : game.find(class_=re.compile('product.+date')).string,
                    u'userscore'    : game.find(class_=re.compile('product.+txt')).find(class_='data').string }

    parsed_game = strip_dict(parsed_game)
    parsed_game[u'title'], parsed_game[u'platform'] = extract_title_and_platform(parsed_game)
    return parsed_game

def extract_games_list(games_table_page):
    try:
        games = []
        for t in [' first', '', ' last']:
            games += games_table_page.find_all(class_='product_row game' + t)
        parsed_games = []
        for game in games:
            try:
                parsed_games.append(parse_single_game_from_table(game))
            except:
                pass
        return parsed_games
    except:
        return []

def parse_game_personal_page(game):
    try:
        page = get_page_soup('http://www.metacritic.com' + game[u'link'])
        summary = page.find('div', class_='score_summary metascore_summary')
        genre_block = page.find('li', class_='summary_detail product_genre')
        publisher_block = page.find('li', class_='summary_detail publisher')
        description_block = page.find('li', class_='summary_detail product_summary')

        #some of those fields may be missing
        try:
            game[u'genre'] = genre_block.find('span', class_='data').string
        except:
            pass
        try:
            game[u'publisher'] = publisher_block.find('span', class_='data').span.string
        except:
            pass
        try:
            game[u'description'] = description_block.find('span', class_='data').find('span', class_='blurb blurb_expanded').string
        except:
            pass
        try:
            game[u'ms_review_count'] = summary.find('div', class_='summary').a.span.string
        except:
            pass
    except:
        pass

    return strip_dict(game)

def process(page_count):
    pool = ThreadPool(processes=24)

    #download pages
    parsed_pages = pool.map(parse_games_table_page, range(page_count))

    #try to get missing pages. Three times
    for retry in range(3):
        for page_num in range(page_count):
            if parsed_pages[page_num] == None:
                parsed_pages[page_num] = parse_games_table_page(page_num)

    #parse games from downloaded pages
    extracted_game_lists = pool.map(extract_games_list, parsed_pages)

    #now we have plenty of lists containing at most 100 games. This is just fine
    sout = getwriter("utf8")(stdout)
    for game_list in extracted_game_lists:
        enriched_games_data = pool.map(parse_game_personal_page, game_list)
        #dump gathered data
        for data in enriched_games_data:
            sout.write(dumps(data, ensure_ascii=False) + "\n")

if __name__ == '__main__':
    page_count = get_page_count()
    if page_count != 0: #fast check for availability
        process(page_count)
