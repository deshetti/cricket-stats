import scrapy
from scrapy import Request
import psycopg2
from datetime import datetime, date, timedelta

class MenTestAllRounderRankingsSpider(scrapy.Spider):
    name = "menTestAllRounderRankings"

    def start_requests(self):
        start_date = date(1877, 3, 19)
        end_date = date(2018, 3, 7)
        for i in range(int((end_date - start_date).days)+1):
            currentdate = start_date + timedelta(i)
            day = currentdate.strftime('%d')
            month = currentdate.strftime('%m')
            year = currentdate.strftime('%Y')
            yield Request('http://www.relianceiccrankings.com/datespecific/test/?stattype=all-rounder&day=%s&month=%s&year=%s' % (day,month,year), callback=self.parse)

    def parse(self, response):
        table = response.css('table.top100table')
        rows = table.css('tr')

        # connect to the PostgreSQL database
        conn = psycopg2.connect("host=localhost port=5432 dbname=opendata user=cricstats password=cricstats")
        # create a new cursor
        cur = conn.cursor()

        # define sql to be executed
        sql = """INSERT INTO "cricstats"."icc_ranking"(ranking,rating,full_name,country,sex,date,format,icc_id,ranking_type) 
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        # Hard-coded values that will change across the other scripts
        sex = "Male"
        gameformat = "Test"
        rankingtype = "All Rounder"

        for i in range(2,len(rows)):
            ranking = rows[i].css('td.top100id::text').extract_first()
            rating = rows[i].css('td.top100rating::text').extract_first()
            fullname = rows[i].css('td.top100name > a::text').extract_first()
            country = rows[i].css('td.top100nation::attr(title)').extract_first()
            rankingdate = response.css('#pagetitle > h2::text').extract_first().replace('ICC Test Championship All-Rounders - ','')
            iccid = rows[i].css('td.top100name > a::attr(href)').extract_first().replace('/playerdisplay/test/all-rounder/?id=','')
            # rankingdate = rankingdate
            rankingdateformatted = datetime.strptime(rankingdate, '%d %B %Y').date()

            # execute the sql statement to insert row in db
            cur.execute(sql, (ranking,rating,fullname,country,sex,rankingdateformatted,gameformat,iccid,rankingtype))

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()