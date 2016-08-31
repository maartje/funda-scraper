import scrapy

class FundaItem(scrapy.Item):
    
    # scraped text fields 
    url = scrapy.Field()
    title = scrapy.Field()
    vraagprijs_text = scrapy.Field()
    periodieke_bijdrage_text = scrapy.Field()
    vve_bijdrage_text = scrapy.Field()
    service_kosten_text = scrapy.Field()
    bouwjaar_text = scrapy.Field()
    bouwperiode_text = scrapy.Field()
    woonoppervlakte_text = scrapy.Field()
    kamers_text = scrapy.Field()

    aangeboden_sinds_text = scrapy.Field()
    verkoopdatum_text = scrapy.Field()
    looptijd_text = scrapy.Field()

    toegankelijkheid_text = scrapy.Field()
    keurmerken_text = scrapy.Field() 

    soort_huis = scrapy.Field()
    soort_appartement = scrapy.Field()

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
    tuin_text = scrapy.Field()
    achtertuin_text = scrapy.Field()
    voortuin_text = scrapy.Field()
    patio_text = scrapy.Field()
    zijtuin_text = scrapy.Field()
    zonneterras_text = scrapy.Field()

    ligging_tuin_text = scrapy.Field()
    balkon_of_dakterras = scrapy.Field()
    schuur_of_berging = scrapy.Field()
    garage = scrapy.Field()
    garage_capaciteit_text = scrapy.Field()
    parkeergelegenheid = scrapy.Field()


    # analyzed fields 
    woningtype = scrapy.Field()  # url
    soort_woning = scrapy.Field()

    gemeente = scrapy.Field()    # title
    postcode = scrapy.Field()    # title
    postcode_regio = scrapy.Field()   # title
    postcode_wijk = scrapy.Field()    # title
    straat = scrapy.Field()      # title
    huisnummer = scrapy.Field()  # title

    vraagprijs = scrapy.Field()  
    kosten_koper = scrapy.Field()

    aangeboden_sinds = scrapy.Field()
    verkoopdatum = scrapy.Field()

    bouwjaar = scrapy.Field()    
    bouwperiode_start = scrapy.Field()
    bouwperiode_end = scrapy.Field()
    
    woonoppervlakte = scrapy.Field() 
    
    perceel_oppervlakte = scrapy.Field() 
    
    inpandige_ruimte = scrapy.Field() 

    buitenruimte = scrapy.Field()
    
    kamers = scrapy.Field()      # kamers_text
    slaapkamers = scrapy.Field() # kamers_text
    
    badkamers = scrapy.Field() # badkamers_text
    toiletten = scrapy.Field() # badkamers_text
    
    periodieke_bijdrage = scrapy.Field()
    
    frans_balkon = scrapy.Field() # balkon_of_dakterras
    dakterras = scrapy.Field()    # balkon_of_dakterras
    balkon = scrapy.Field()       # balkon_of_dakterras
    
    voortuin_diepte = scrapy.Field()
    voortuin_breedte = scrapy.Field()
    voortuin_oppervlakte = scrapy.Field()

    achtertuin_diepte = scrapy.Field()
    achtertuin_breedte = scrapy.Field()
    achtertuin_oppervlakte = scrapy.Field()

    patio_diepte = scrapy.Field()
    patio_breedte = scrapy.Field()
    patio_oppervlakte = scrapy.Field()

    zijtuin_diepte = scrapy.Field()
    zijtuin_breedte = scrapy.Field()
    zijtuin_oppervlakte = scrapy.Field()

    zonneterras_diepte = scrapy.Field()
    zonneterras_breedte = scrapy.Field()
    zonneterras_oppervlakte = scrapy.Field()

    ligging_tuin = scrapy.Field()
    achterom = scrapy.Field()
    
    achtertuin = scrapy.Field()
    voortuin = scrapy.Field()
    zijtuin = scrapy.Field()
    tuin_rondom = scrapy.Field()
    patio = scrapy.Field()
    zonneterras = scrapy.Field()
    plaats = scrapy.Field()


    woonlagen = scrapy.Field()
    kelder = scrapy.Field()
    vliering = scrapy.Field()
    zolder = scrapy.Field()

    garage_capaciteit = scrapy.Field()

    
    externe_bergruimte = scrapy.Field()
    verdieping = scrapy.Field()

    inhoud = scrapy.Field()
    
    eigendomssituatie = scrapy.Field()
    eind_datum_erfpacht = scrapy.Field()
    kosten_erfpacht = scrapy.Field()

    energielabel = scrapy.Field()
    
    
    # omschrijving
    # image url
    
    
