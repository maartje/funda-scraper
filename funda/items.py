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
    
    inhoud_text = scrapy.Field()
    woonlagen_text = scrapy.Field()
    badkamers_text = scrapy.Field()
    gelegen_op_text = scrapy.Field()
    badkamervoorzieningen = scrapy.Field()
    externe_bergruimte_text = scrapy.Field()
    voorzieningen = scrapy.Field()
    
    energielabel_text = scrapy.Field()
    isolatie = scrapy.Field()
    verwarming = scrapy.Field()
    warm_water = scrapy.Field()
    cv_ketel = scrapy.Field()
    eigendomssituatie_text = scrapy.Field()
    lasten_text = scrapy.Field()
    ligging = scrapy.Field()
    tuin = scrapy.Field()
    achtertuin = scrapy.Field()
    voortuin = scrapy.Field()
    ligging_tuin = scrapy.Field()
    balkon_of_dakterras = scrapy.Field()
    schuur_of_berging = scrapy.Field()
    garage = scrapy.Field()
    garage_capaciteit = scrapy.Field()
    parkeergelegenheid = scrapy.Field()


    # analyzed fields 
    woningtype = scrapy.Field()  # url

    gemeente = scrapy.Field()    # title
    postcode = scrapy.Field()    # title
    straat = scrapy.Field()      # title
    huisnummer = scrapy.Field()  # title

    vraagprijs = scrapy.Field()  
    kosten_koper = scrapy.Field()

    bouwjaar = scrapy.Field()    
    
    woonoppervlakte = scrapy.Field() 
    
    perceel_oppervlakte = scrapy.Field() 
    
    inpandige_ruimte = scrapy.Field() 

    buitenruimte = scrapy.Field()
    
    kamers = scrapy.Field()      # kamers_text
    slaapkamers = scrapy.Field() # kamers_text
    
    
    periodieke_bijdrage = scrapy.Field()
    
    
    
    # mj = scrapy.Field()
    
    
