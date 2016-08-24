import scrapy

class FundaItem(scrapy.Item):
    
    # scraped text fields 
    url = scrapy.Field()
    title = scrapy.Field()
    vraagprijs_text = scrapy.Field()
    periodieke_bijdrage_text = scrapy.Field()
    bouwjaar_text = scrapy.Field()
    woonoppervlakte_text = scrapy.Field()
    kamers_text = scrapy.Field()
    soort_woning = scrapy.Field()
    soort_bouw = scrapy.Field()
    soort_dak = scrapy.Field()
    specifiek = scrapy.Field()
    perceel_oppervlakte_text = scrapy.Field()
    inpandige_ruimte_text = scrapy.Field()
    buitenruimte_text = scrapy.Field()
    status = scrapy.Field()
    aanvaarding = scrapy.Field()


    # analyzed fields 
    woningtype = scrapy.Field()  # url

    gemeente = scrapy.Field()    # title
    postcode = scrapy.Field()    # title
    address = scrapy.Field()     # title
    straat = scrapy.Field()      # title
    huisnummer = scrapy.Field()  # title

    vraagprijs = scrapy.Field()  # vraagprijs_text

    bouwjaar = scrapy.Field()    # bouwjaar_text
    
    woonoppervlakte = scrapy.Field() # woonoppervlakte_text
    
    kamers = scrapy.Field()      # kamers_text
    slaapkamers = scrapy.Field() # kamers_text
    
    
    
    
    
    mj = scrapy.Field()
    
    
