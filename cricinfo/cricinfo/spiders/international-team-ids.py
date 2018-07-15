import scrapy
from scrapy import Request
import psycopg2
from datetime import datetime

class InternationalTeamIds(scrapy.Spider):
    name = "internationalTeamIds"

    def start_requests(self):
        yield Request('http://stats.espncricinfo.com/ci/engine/stats/index.html?class=11;template=results;type=team', callback=self.parse)

    def parse(self, response):
        table = response.css('table.engineTable > tbody')
        rows = table.css('tr')
        # connect to the PostgreSQL database
        conn = psycopg2.connect("host=localhost port=5432 dbname=opendata user=cricstats password=cricstats")
        # create a new cursor
        cur = conn.cursor()

        # define sql to be executed
        sql = """INSERT INTO "cricstats"."ci_international_team"(name,ciid,last_updated) 
                VALUES(%s,%s,%s)"""

        for i in range(len(rows)):
            name = rows[i].css('td:nth-child(1) > a::text').extract_first()
            ciid = int(rows[i].css('td:nth-child(1) > a::attr(href)').extract_first().replace('/ci/content/team/','').replace('.html',''))
            last_updated = datetime.now()

            # execute the sql statement to insert row in db
            cur.execute(sql, (name,ciid,last_updated))

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()