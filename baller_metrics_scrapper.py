import scrapy
from scrapy.crawler import CrawlerProcess 


# create list of a to z, excluding x
strings = string.ascii_lowercase[:23] + "yz"

base_url = "https://www.basketball-reference.com/players/"

#Add Base 
url_list= [base_url + string for string in strings]



# Create the Spider class
class BasketSpider(scrapy.Spider):
    name = 'BasketSpider'
    # start_requests method
    def start_requests( self ):
        #Batched manually by adjusting the index below
        for url in url_list:
            yield scrapy.Request(url ,callback = self.parse_getplayers)
          
    

    def parse_getplayers(self, response):
        # get links to all bold letters
        players_links = response.xpath('//tbody//tr//strong/a/@href').extract()
        #follow link
        for player_link in players_links:
            yield response.follow(url = player_link, callback = self.parse_getseasons)
    
    
    def parse_getseasons(self, response):
        # get links to all bold letters
        season_links = response.xpath('//div[@id="inner_nav"]/ul/li[2]/div/ul[1]//@href').extract()
        #follow link
        for season_link in season_links:
            gamelog_table = season_link
            yield response.follow(url = gamelog_table, callback = self.parse)
        
    def parse(self, response):
        #parse player name
        player_name = response.xpath('//div[@id="inner_nav"]/ul/li[1]/a/u').extract_first()[3:-13]
        
        game_stats = response.xpath('//table/tbody//tr')
        # ignore the table header row
        for game in game_stats[1:]:
            three_pct = game.xpath('td[15]//text()').extract_first()
            three = game.xpath('td[13]//text()').extract_first()
            threePA = game.xpath('td[14]//text()').extract_first()
            plus_minus = game.xpath('td[29]//text()').extract_first()
            FG_pct = game.xpath('td[12]//text()').extract_first()
            FT_pct = game.xpath('td[18]//text()').extract_first()
            G    =  game.xpath('td[1]//text()').extract_first()
            Date =  game.xpath('td[2]//text()').extract_first()
            Age  =  game.xpath('td[3]//text()').extract_first()
            Team =  game.xpath('td[4]//text()').extract_first()
            At   =  game.xpath('td[5]//text()').extract_first()
            Opp  =  game.xpath('td[6]//text()').extract_first()
            Form =  game.xpath('td[7]//text()').extract_first()
            GS   =  game.xpath('td[8]//text()').extract_first()
            MP   =  game.xpath('td[9]//text()').extract_first()
            FG   =  game.xpath('td[10]//text()').extract_first()
            FGA  =  game.xpath('td[11]//text()').extract_first() 
            FT   =  game.xpath('td[16]//text()').extract_first()
            FTA  =  game.xpath('td[17]//text()').extract_first() 
            ORB  =  game.xpath('td[19]//text()').extract_first()
            DRB  =  game.xpath('td[20]//text()').extract_first()
            TRB  =  game.xpath('td[21]//text()').extract_first()
            AST  =  game.xpath('td[22]//text()').extract_first()
            STL  =  game.xpath('td[23]//text()').extract_first()
            BLK  =  game.xpath('td[24]//text()').extract_first()
            TOV  =  game.xpath('td[25]//text()').extract_first()
            PF   =  game.xpath('td[26]//text()').extract_first()
            PTS  =  game.xpath('td[27]//text()').extract_first()
            GmSc =  game.xpath('td[28]//text()').extract_first()
            name =  player_name
            if Date is None:
                key = name + '0'
            else:
                key = name + " / " + Date
            #dates are unique for each player. A unique key is name + date
            player_stats[key] =[G, name, Date, Age, Team, At, Opp, Form, GS, MP, FG, FGA, FG_pct, three, threePA, three_pct, FT, FTA, FT_pct, ORB, DRB, TRB, AST, STL, BLK, TOV, PF, PTS, GmSc, plus_minus]
            

player_stats = {}
              
# Run the Spider
process = CrawlerProcess()
process.crawl(BasketSpider)
process.start()

len(player_stats)

#convert dictionary of stats into table
df1 = pd.DataFrame.from_dict(player_stats).transpose()

df1 = df1.reset_index(level=0)

#rename columns in a table
df1.columns = [
 'key', 'game_index','name','Date','Age','home_team_id','At','away_team_id','Form',
 'GS','MP','FG','FGA','FG_pct','three',
 'threePA','three_pct','FT','FTA',
 'FT_pct','ORB','DRB','TRB','AST','STL',
 'BLK','TOV','PF','PTS','GmSc','plus_minus'
]

# some rows have information on wheter plays played or not stored in `GS` column
df1 = df1[~((df1.GS== "Did Not Play")| (df1.GS == "Inactive"))]

# others have information stored in `MP` column
df1 = df1[~((df1.MP == "Did Not Play")| (df1.MP == "Inactive"))]

df1 = df1.drop('At', axis=1)

# Certain rows dont have any values at all. 
# However every row that conatains some values have their date present, we can use this fact to eliminate rows with no value.
df1 = df1[~(df1.Date.isna())]

df1.to_csv("player_stat.csv")