import scrapy
from scrapy import Request
import psycopg2
from datetime import datetime

class PlayerBio(scrapy.Spider):
    name = "playerBio"

    def start_requests(self):
        # connect to the PostgreSQL database
        conn = psycopg2.connect("host=localhost port=5432 dbname=opendata user=cricstats password=cricstats")
        # create a new cursor
        cur = conn.cursor()
        # define sql to be executed
        sql = """SELECT ciid from "cricstats"."player" """
        # execute the sql statement to insert row in db
        cur.execute(sql)
        # Fetch results and store in a list variable
        player_ciids = cur.fetchall()
        # close communication with the database
        cur.close()
        for player_ciid in player_ciids:
            yield Request('http://www.espncricinfo.com/australia/content/player/%s.html' %(player_ciid), callback=self.parse, meta={'player_ciid': player_ciid})
            # yield Request('http://www.espncricinfo.com/australia/content/player/35320.html', callback=self.parse, meta={'player_ciid': 35320})
            # yield Request('http://www.espncricinfo.com/australia/content/player/974113.html', callback=self.parse, meta={'player_ciid': 974113})

    def parse(self, response):
        rows = response.css('p.ciPlayerinformationtxt')
        # connect to the PostgreSQL database
        conn = psycopg2.connect("host=localhost port=5432 dbname=opendata user=cricstats password=cricstats")
        # create a new cursor
        cur = conn.cursor()

        # define sql to be executed
        player_exists_sql = """SELECT COUNT(*) FROM "cricstats"."player" WHERE ciid = %s"""
        insert_sql = """INSERT INTO "cricstats"."player"(full_name,nick_name,dob,born_city, major_teams, playing_role, batting_style, bowling_style, height, education, last_updated, country) 
              VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        update_sql = """UPDATE "cricstats"."player" 
                SET full_name = %s,nick_name = %s,dob = %s,born_city = %s, major_teams = %s, playing_role = %s, batting_style = %s, bowling_style = %s, 
                height = %s, education = %s, last_updated = %s, country = %s WHERE ciid = %s"""

        full_name = nick_name = dob = born_city = major_teams = playing_role = batting_style = bowling_style = height = education = None
        country = response.css('h3.PlayersSearchLink > b::text').extract_first()
        for i in range(len(rows)):
            label = rows[i].css('b::text').extract_first()
            bio = rows[i].css('span::text').extract()
            if label == 'Full name':
                full_name = bio[0].replace('\n','').strip()
            if label == 'Born':
                try:
                    if bio[0].replace('\n','').strip() != 'date unknown':
                        dobandpob = bio[0].replace('\n','').strip().split(',')
                        dobstring = dobandpob[0]+', '+dobandpob[1]
                        dob = datetime.strptime(dobstring,'%B %d, %Y').date()
                        pob = dobandpob[2:]
                        born_city = ",".join(pob).strip()
                except:
                    pass
            if label == 'Major teams':
                major_teams = ''.join(bio).strip()
            if label == 'Nickname':
                nick_name = bio[0].replace('\n','').strip()
            if label == 'Playing role':
                playing_role = bio[0].replace('\n','').strip()
            if label == 'Batting style':
                batting_style = bio[0].replace('\n','').strip()
            if label == 'Bowling style':
                bowling_style = bio[0].replace('\n','').strip()
            if label == 'Height':
                #calculate inches vs mt
                height = bio[0].replace('\n','').strip()
            if label == 'Education':
                education = bio[0].replace('\n','').strip()

        last_updated = datetime.now()
        if player_exists_sql == 0:
            cur.execute(insert_sql, (full_name,nick_name,dob,born_city, major_teams, playing_role, batting_style, bowling_style, height, education, last_updated, country))
        else:
            cur.execute(update_sql, (full_name,nick_name,dob,born_city, major_teams, playing_role, batting_style, bowling_style, height, education, last_updated, country, response.meta['player_ciid']))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()