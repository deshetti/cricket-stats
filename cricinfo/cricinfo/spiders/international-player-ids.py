import scrapy
from scrapy import Request
import psycopg2
from datetime import datetime

class InternationalPlayerIds(scrapy.Spider):
    name = "internationalPlayerIds"

    def start_requests(self):
        # connect to the PostgreSQL database
        conn = psycopg2.connect("host=localhost port=5432 dbname=opendata user=cricstats password=cricstats")
        # create a new cursor
        cur = conn.cursor()
        # define sql to be executed
        sql = """SELECT ciid from "cricstats"."ci_international_teams" """
        # execute the sql statement to insert row in db
        cur.execute(sql)
        # Fetch results and store in a list variable
        team_ciids = cur.fetchall()
        # close communication with the database
        cur.close()
        for team_ciid in team_ciids:
            yield Request('http://www.espncricinfo.com/ci/content/player/caps.html?country=%s;class=1' %(team_ciid), callback=self.parse, meta={'team_ciid': team_ciid})
            yield Request('http://www.espncricinfo.com/ci/content/player/caps.html?country=%s;class=2' %(team_ciid), callback=self.parse, meta={'team_ciid': team_ciid})
            yield Request('http://www.espncricinfo.com/ci/content/player/caps.html?country=%s;class=3' %(team_ciid), callback=self.parse, meta={'team_ciid': team_ciid})

    def parse(self, response):
        rows = response.css('li.ciPlayername')
        # connect to the PostgreSQL database
        conn = psycopg2.connect("host=localhost port=5432 dbname=opendata user=cricstats password=cricstats")
        # create a new cursor
        cur = conn.cursor()

        # define sql to check if player exists already
        player_exists_sql = """SELECT COUNT(*) FROM "cricstats"."player" WHERE ciid = %s"""
        # define sql to be executed
        sql = """INSERT INTO "cricstats"."player"(ci_name,ciid,last_updated,international_team_ciid) VALUES(%s,%s,%s,%s)"""

        for i in range(len(rows)):
            ci_name = rows[i].css('a::text').extract_first()
            ciid = int(rows[i].css('a::attr(href)').extract_first().replace('/ci/content/player/','').replace('.html',''))
            last_updated = datetime.now()

            # Check if the player exists in the DB
            cur.execute(player_exists_sql,[ciid])
            count = cur.fetchone()

            # Insert row in DB if player doesn't exist
            if count[0] is 0:
                # execute the sql statement to insert row in db
                cur.execute(sql, (ci_name,ciid,last_updated,response.meta['team_ciid']))

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()