import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from funda.items import FundaItem
import re

class FundaSpider(CrawlSpider):

    name = "funda_spider"
    allowed_domains = ["funda.nl"]


    def __init__(self, place='amsterdam'):
        self.start_urls = ["http://www.funda.nl/koop/%s/p%s/" % (place, page_number) for page_number in range(250,500)]
        self.base_url = "http://www.funda.nl/koop/%s/" % place
        self.le1 = LinkExtractor(allow=r'%s+(huis|appartement)-\d{8}' % self.base_url)

    def parse(self, response):
        page_nr_matches = re.findall(r'p(\d+)', response.url, re.IGNORECASE)
        page_nr = int(page_nr_matches[0]) if page_nr_matches else 0
        links = self.le1.extract_links(response)
        for link in links:
            if link.url.count('/') == 6 and link.url.endswith('/'):
                item = FundaItem()
                item['url'] = link.url
                item['page_nr'] = page_nr
                yield scrapy.Request(link.url, callback=self.parse_dir_contents, meta={'item': item})

    def parse_dir_contents(self, response):

        new_item = response.request.meta['item']

        new_item['title'] = response.xpath('//title/text()').extract()[0]

        new_item['vraagprijs_text'] = self.extract_feature(response, 'Vraagprijs')
        
        new_item['aangeboden_sinds_text'] = self.extract_feature(response, 'Aangeboden sinds')

        new_item['verkoopdatum_text'] = self.extract_feature(response, 'Verkoopdatum')

        new_item['looptijd'] = self.extract_feature(response, 'Looptijd')

        new_item['toegankelijkheid'] = self.extract_feature(response, 'Toegankelijkheid')

        new_item['keurmerken'] = self.extract_feature(response, 'Keurmerken')

        new_item['bouwjaar_text'] = self.extract_feature(response, 'Bouwjaar')

        new_item['bouwperiode_text'] = self.extract_feature(response, 'Bouwperiode')

        new_item['woonoppervlakte_text'] = self.extract_feature(response, 'Woonoppervlakte')

        new_item['kamers_text'] = self.extract_feature(response, 'Aantal kamers')

        new_item['status'] =  self.extract_feature(response, 'Status')

        new_item['aanvaarding'] = self.extract_feature(response, 'Aanvaarding') 

        new_item['vve_bijdrage_text'] = self.extract_feature(response, 'Bijdrage VvE')

        new_item['periodieke_bijdrage_text'] = self.extract_feature(response, 'Periodieke bijdrage')

        new_item['service_kosten_text'] = self.extract_feature(response, 'Servicekosten')

        new_item['soort_huis'] = self.extract_feature(response, 'Soort woonhuis')

        new_item['soort_appartement'] = self.extract_feature(response, 'Soort appartement')

        new_item['soort_bouw'] =  self.extract_feature(response, 'Soort bouw')

        new_item['soort_dak'] = self.extract_feature(response, 'Soort dak')

        new_item['specifiek'] = self.extract_feature(response, 'Specifiek') 

        new_item['perceel_oppervlakte_text'] = self.extract_feature(response, 'Perceeloppervlakte')

        new_item['inpandige_ruimte_text'] = self.extract_feature(response, 'inpandige ruimte')

        new_item['buitenruimte_text'] = self.extract_feature(response, 'Gebouwgebonden buitenruimte')

        new_item['inhoud_text'] = self.extract_feature(response, 'Inhoud')

        new_item['woonlagen_text'] = self.extract_feature(response, 'Aantal woonlagen')

        new_item['badkamers_text'] = self.extract_feature(response, 'Aantal badkamers')

        new_item['gelegen_op_text'] = self.extract_feature(response, 'Gelegen op')

        new_item['badkamervoorzieningen'] = self.extract_feature(response, 'Badkamervoorzieningen')

        new_item['externe_bergruimte_text'] = self.extract_feature(response, 'Externe bergruimte')

        new_item['voorzieningen'] = self.extract_feature(response, 'Voorzieningen')

        new_item['energielabel_text'] = self.extract_text(response, "//span[contains(@class, 'energielabel')]/text()")

        new_item['isolatie'] = self.extract_feature(response, 'Isolatie')

        new_item['verwarming'] = self.extract_feature(response, 'Verwarming')

        new_item['warm_water'] = self.extract_feature(response, 'Warm water')

        new_item['cv_ketel'] = self.extract_feature(response, 'Cv-ketel')

        # can occur two times on a page, we combine both occurences to a single string
        new_item['eigendomssituatie_text'] = self.extract_text(response, "//dt[contains(.,'Eigendomssituatie')]/following-sibling::dd[1]/text()")

        # can occur two times on a page, we combine both occurences to a single string
        new_item['lasten_text'] = self.extract_text(response, "//dt[contains(.,'Lasten')]/following-sibling::dd[1]/text()")

        new_item['ligging'] = self.extract_feature(response, 'Ligging')

        new_item['tuin_text'] = self.extract_feature(response, 'Tuin')

        new_item['achtertuin_text'] = self.extract_feature(response, 'Achtertuin')

        new_item['voortuin_text'] = self.extract_feature(response, 'Voortuin')

        new_item['patio_text'] = self.extract_feature(response, 'Patio')

        new_item['zijtuin_text'] = self.extract_feature(response, 'Zijtuin')

        new_item['zonneterras_text'] = self.extract_feature(response, 'Zonneterras')


        new_item['ligging_tuin_text'] = self.extract_feature(response, 'Ligging tuin')

        new_item['balkon_of_dakterras'] = self.extract_feature(response, 'Balkon/dakterras')

        new_item['schuur_of_berging'] = self.extract_feature(response, 'Schuur/berging')

        new_item['garage_text'] = self.extract_feature(response, 'Soort garage')

        new_item['garage_capaciteit_text'] = self.extract_feature(response, 'Capaciteit')

        new_item['parkeergelegenheid'] = self.extract_feature(response, 'Soort parkeergelegenheid')

        yield new_item
        
    def extract_feature(self, response, keyword):
        return self.extract_text(response, "(//dt[contains(.,'" + keyword + "')]/following-sibling::dd[1])[1]/text()")

    def extract_text(self, response, xpath):
        results = response.xpath(xpath).extract()
        results = map(lambda x: x.strip(), results)
        return ' '.join(results).strip().lower()


# Omschrijving
# Image url

