import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from funda.items import FundaItem

class FundaSpider(CrawlSpider):

    name = "funda_spider"
    allowed_domains = ["funda.nl"]

    def __init__(self, place='amsterdam'):
        self.start_urls = ["http://www.funda.nl/koop/%s/p%s/" % (place, page_number) for page_number in range(1,10)]
        self.base_url = "http://www.funda.nl/koop/%s/" % place
        self.le1 = LinkExtractor(allow=r'%s+(huis|appartement)-\d{8}' % self.base_url)

    def extract_text(self, response, xpath):
        results = response.xpath(xpath).extract()
        return results[0].strip().lower() if results else ''

    def parse(self, response):
        links = self.le1.extract_links(response)
        for link in links:
            if link.url.count('/') == 6 and link.url.endswith('/'):
                item = FundaItem()
                item['url'] = link.url
                yield scrapy.Request(link.url, callback=self.parse_dir_contents, meta={'item': item})

    def parse_dir_contents(self, response):

        new_item = response.request.meta['item']

        new_item['title'] = response.xpath('//title/text()').extract()[0]

        new_item['vraagprijs_text'] = self.extract_text(response, "//dt[contains(.,'Vraagprijs')]/following-sibling::dd[1]/text()")

        new_item['bouwjaar_text'] = self.extract_text(response, "//dt[contains(.,'Bouwjaar')]/following-sibling::dd[1]/text()")

        new_item['woonoppervlakte_text'] = self.extract_text(response, "//dt[contains(.,'Woonoppervlakte')]/following-sibling::dd[1]/text()")

        new_item['kamers_text'] = self.extract_text(response, "//dt[contains(.,'Aantal kamers')]/following-sibling::dd[1]/text()")

        new_item['status'] =  self.extract_text(response, "//dt[contains(.,'Status')]/following-sibling::dd[1]/text()")

        new_item['aanvaarding'] =  self.extract_text(response, "//dt[contains(.,'Aanvaarding')]/following-sibling::dd[1]/text()")
        
        new_item['vve_bijdrage_text'] = self.extract_text(response, "//dt[contains(.,'Bijdrage VvE')]/following-sibling::dd[1]/text()")
        
        new_item['periodieke_bijdrage_text'] = self.extract_text(response, "//dt[contains(.,'Periodieke bijdrage')]/following-sibling::dd[1]/text()")
        
        new_item['service_kosten_text'] = self.extract_text(response, "//dt[contains(.,'Servicekosten')]/following-sibling::dd[1]/text()")

        new_item['soort_huis'] = self.extract_text(response, "//dt[contains(.,'Soort woonhuis')]/following-sibling::dd[1]/text()")
        
        new_item['soort_appartement'] = self.extract_text(response, "//dt[contains(.,'Soort appartement')]/following-sibling::dd[1]/text()")

        new_item['soort_bouw'] =  self.extract_text(response, "//dt[contains(.,'Soort bouw')]/following-sibling::dd[1]/text()")

        new_item['soort_dak'] =  self.extract_text(response, "//dt[contains(.,'Soort dak')]/following-sibling::dd[1]/text()")

        new_item['specifiek'] =  self.extract_text(response, "//dt[contains(.,'Specifiek')]/following-sibling::dd[1]/text()")

        new_item['perceel_oppervlakte_text'] =  self.extract_text(response, "//dt[contains(.,'Perceeloppervlakte')]/following-sibling::dd[1]/text()")

        new_item['inpandige_ruimte_text'] =  self.extract_text(response, "//dt[contains(.,'Overige inpandige ruimte')]/following-sibling::dd[1]/text()")

        new_item['buitenruimte_text'] =  self.extract_text(response, "//dt[contains(.,'Gebouwgebonden buitenruimte')]/following-sibling::dd[1]/text()")

        new_item['inhoud_text'] =  self.extract_text(response, "//dt[contains(.,'Inhoud')]/following-sibling::dd[1]/text()")

        new_item['woonlagen_text'] =  self.extract_text(response, "//dt[contains(.,'Aantal woonlagen')]/following-sibling::dd[1]/text()")

        new_item['badkamers_text'] =  self.extract_text(response, "//dt[contains(.,'Aantal badkamers')]/following-sibling::dd[1]/text()")

        new_item['gelegen_op_text'] =  self.extract_text(response, "//dt[contains(.,'Gelegen op')]/following-sibling::dd[1]/text()")

        new_item['badkamervoorzieningen'] =  self.extract_text(response, "//dt[contains(.,'Badkamervoorzieningen')]/following-sibling::dd[1]/text()")

        new_item['externe_bergruimte_text'] =  self.extract_text(response, "//dt[contains(.,'Externe bergruimte')]/following-sibling::dd[1]/text()")

        new_item['voorzieningen'] =  self.extract_text(response, "//dt[contains(.,'Voorzieningen')]/following-sibling::dd[1]/text()")

        new_item['energielabel_text'] =  self.extract_text(response, "//span[contains(@class, 'energielabel')]/text()")

        new_item['isolatie'] =  self.extract_text(response, "//dt[contains(.,'Isolatie')]/following-sibling::dd[1]/text()")
        
        new_item['verwarming'] =  self.extract_text(response, "//dt[contains(.,'Verwarming')]/following-sibling::dd[1]/text()")

        new_item['warm_water'] =  self.extract_text(response, "//dt[contains(.,'Warm water')]/following-sibling::dd[1]/text()")

        new_item['cv_ketel'] =  self.extract_text(response, "//dt[contains(.,'Cv-ketel')]/following-sibling::dd[1]/text()")

        new_item['eigendomssituatie_text'] =  self.extract_text(response, "//dt[contains(.,'Eigendomssituatie')]/following-sibling::dd[1]/text()")

        new_item['lasten_text'] =  self.extract_text(response, "//dt[contains(.,'Lasten')]/following-sibling::dd[1]/text()")

        new_item['ligging'] =  self.extract_text(response, "//dt[contains(.,'Ligging')]/following-sibling::dd[1]/text()")

        new_item['tuin_text'] =  self.extract_text(response, "//dt[contains(.,'Tuin')]/following-sibling::dd[1]/text()")

        new_item['achtertuin_text'] =  self.extract_text(response, "//dt[contains(.,'Achtertuin')]/following-sibling::dd[1]/text()")

        new_item['voortuin_text'] =  self.extract_text(response, "//dt[contains(.,'Voortuin')]/following-sibling::dd[1]/text()")

        new_item['ligging_tuin_text'] =  self.extract_text(response, "//dt[contains(.,'Ligging tuin')]/following-sibling::dd[1]/text()")

        new_item['balkon_of_dakterras'] =  self.extract_text(response, "//dt[contains(.,'Balkon/dakterras')]/following-sibling::dd[1]/text()")

        new_item['schuur_of_berging'] =  self.extract_text(response, "//dt[contains(.,'Schuur/berging')]/following-sibling::dd[1]/text()")

        new_item['garage'] =  self.extract_text(response, "//dt[contains(.,'Soort garage')]/following-sibling::dd[1]/text()")

        new_item['garage_capaciteit_text'] =  self.extract_text(response, "//dt[contains(.,'Capaciteit')]/following-sibling::dd[1]/text()")

        new_item['parkeergelegenheid'] =  self.extract_text(response, "//dt[contains(.,'Soort parkeergelegenheid')]/following-sibling::dd[1]/text()")

        yield new_item
        


# Omschrijving
# Image url

