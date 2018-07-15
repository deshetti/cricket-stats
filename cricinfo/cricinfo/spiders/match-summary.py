import scrapy
from scrapy import Request
import psycopg2
from datetime import datetime, date, timedelta
from daterangeparser import parse as dateparser

class MatchSummary(scrapy.Spider):
    name = "matchSummary"

    def start_requests(self):
        start_year = 2007
        end_year = 2007
        for i in range((end_year - start_year)+1):
            year = start_year + i
            yield Request('http://stats.espncricinfo.com/ci/engine/records/team/match_results.html?class=13;id=%s;type=year' % (year), callback=self.parse)

    def parse(self, response):
        table = response.css('table.engineTable > tbody')
        rows = table.css('tr')
        # connect to the PostgreSQL database
        conn = psycopg2.connect("host=localhost port=5432 dbname=opendata user=cricstats password=cricstats")
        # create a new cursor
        cur = conn.cursor()

        # define sql to be executed
        sql = """INSERT INTO "cricstats"."ci_match"(team1,team1_ciid,team2,team2_ciid,winner,winner_ciid,margin,ground,ground_ciid,start_date,end_date,match_type,match_type_ciid,match_number,match_ciid) 
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in range(len(rows)):
            team1 = rows[i].css('td:nth-child(1) > a::text').extract_first()
            team1_ciid =  int(rows[i].css('td:nth-child(1) > a::attr(href)').extract_first().replace('/ci/content/team/','').replace('.html',''))
            team2 = rows[i].css('td:nth-child(2) > a::text').extract_first()
            team2_ciid = rows[i].css('td:nth-child(2) > a::attr(href)').extract_first().replace('/ci/content/team/','').replace('.html','')
            winner = rows[i].css('td:nth-child(3) > a::text').extract_first()
            if not winner:
                winner = rows[i].css('td:nth-child(3)::text').extract_first()
            winner_ciid = rows[i].css('td:nth-child(3) > a::attr(href)').extract_first()
            if winner_ciid:
                winner_ciid = winner_ciid.replace('/ci/content/team/','').replace('.html','')
            margin = rows[i].css('td:nth-child(4)::text').extract_first()
            ground = rows[i].css('td:nth-child(5) > a::text').extract_first()
            ground_ciid = rows[i].css('td:nth-child(5) > a::attr(href)').extract_first().replace('/ci/content/ground/','').replace('.html','')

            # Scrape start and end dates of the match
            dates = rows[i].css('td:nth-child(6)::text').extract_first()
            if '-' in dates:
                start_date_string, end_date_string = dateparser(dates)
                start_date = start_date_string.date()
                end_date = end_date_string.date()
            else:
                start_date = datetime.strptime(dates,'%b %d, %Y').date()
                end_date = datetime.strptime(dates,'%b %d, %Y').date()

            match_number = None
            match_type_number = rows[i].css('td:nth-child(7) > a::text').extract_first().split(' # ')
            match_type_ciid = 13
            if len(match_type_number) > 1:
                match_type = match_type_number[0]
                match_number = match_type_number[1]
            else:
                match_type = match_type_number[0]

            match_ciid = rows[i].css('td:nth-child(7) > a::attr(href)').extract_first().replace('/ci/engine/match/','').replace('.html','')

            # execute the sql statement to insert row in db
            cur.execute(sql, (team1,team1_ciid,team2,team2_ciid,winner,winner_ciid,margin,ground,ground_ciid,start_date,end_date,match_type,match_type_ciid,match_number,match_ciid))

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()