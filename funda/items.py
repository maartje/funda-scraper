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
    soort_woning = scrapy.Field()
    soort_bouw = scrapy.Field()
    perceel_oppervlakte = scrapy.Field()
    
    soort_dak = scrapy.Field()
    specifiek = scrapy.Field()
    inpandige_ruimte = scrapy.Field()
    buitenruimte = scrapy.Field()
    
    status = scrapy.Field()
    aanvaarding = scrapy.Field()
    straat = scrapy.Field()
    huisnummer = scrapy.Field()
    
    title = scrapy.Field()
    periodieke_bijdrage_text = scrapy.Field()
    vraagprijs_text = scrapy.Field()
    bouwjaar_text = scrapy.Field()
    woonoppervlakte_text = scrapy.Field()
    kamers_text = scrapy.Field()
    
    
    
    
