import scrapy

class FundaItem(scrapy.Item):
    url = scrapy.Field()
    woningtype = scrapy.Field()
    postcode = scrapy.Field()
    address = scrapy.Field()
    vraagprijs = scrapy.Field()
    bouwjaar = scrapy.Field()
    woonoppervlakte = scrapy.Field()
    kamers = scrapy.Field()
    slaapkamers = scrapy.Field()
    gemeente = scrapy.Field()
    periodieke_bijdrage = scrapy.Field()
    soort_woning = scrapy.Field()
    soort_bouw = scrapy.Field()
    perceel_oppervlakte = scrapy.Field()
    
    soort_dak = scrapy.Field()
    specifiek = scrapy.Field()
    inpandige_ruimte = scrapy.Field()
    buitenruimte = scrapy.Field()
    
    
    
    
