import re
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from funda.items import FundaItem

class FundaSoldSpider(CrawlSpider):

    name = "funda_spider_sold"
    allowed_domains = ["funda.nl"]

    def __init__(self, place='amsterdam'):
        self.start_urls = ["http://www.funda.nl/koop/verkocht/%s/p%s/" % (place, page_number) for page_number in range(1,10)]
        # self.start_urls = ["http://www.funda.nl/koop/verkocht/%s/p1/" % place]  # For testing, extract just from one page
        self.base_url = "http://www.funda.nl/koop/verkocht/%s/" % place
        self.le1 = LinkExtractor(allow=r'%s+(huis|appartement)-\d{8}' % self.base_url)
        self.le2 = LinkExtractor(allow=r'%s+(huis|appartement)-\d{8}.*/kenmerken/' % self.base_url)

    def parse(self, response):
        links = self.le1.extract_links(response)
        slash_count = self.base_url.count('/')+1        # Controls the depth of the links to be scraped
        for link in links:
            if link.url.count('/') == slash_count and link.url.endswith('/'):
                item = FundaItem()
                item['url'] = link.url
                yield scrapy.Request(link.url, callback=self.parse_dir_contents, meta={'item': item})

    def parse_dir_contents(self, response):
        new_item = response.request.meta['item']
        
        new_item['title'] = response.xpath('//title/text()').extract()[0]
        
        new_item['vraagprijs_text'] = self.extract_text(response, "//span[contains(@class, 'price-wrapper' )]/span[contains(@class, 'price' )]/text()")

        # posting_date = response.xpath("//span[contains(@class, 'transaction-date') and contains(.,'Aangeboden sinds')]/strong/text()").extract()[0]
        # new_item['posting_date'] = posting_date
        # sale_date = response.xpath("//span[contains(@class, 'transaction-date') and contains(.,'Verkoopdatum')]/strong/text()").extract()[0]
        # new_item['sale_date'] = sale_date

        links = self.le2.extract_links(response)
        slash_count = self.base_url.count('/') + 2
        proper_links = filter(lambda link: link.url.count('/')==slash_count and link.url.endswith('/'), links)
        
        if not(proper_links):
            yield new_item
        else:
            yield scrapy.Request(proper_links[0].url, callback=self.parse_details, meta={'item': new_item})

    def parse_details(self, response):
        new_item = response.request.meta['item']

        new_item['bouwjaar_text'] = self.extract_text(response, "//th[contains(.,'Bouwjaar')]/following-sibling::td[1]/span/text()")

        new_item['woonoppervlakte_text'] = self.extract_text(response, "//th[contains(.,'oonoppervlakte')]/following-sibling::td[1]/span/text()")

        new_item['kamers_text'] = self.extract_text(response, "//th[contains(.,'Aantal kamers')]/following-sibling::td[1]/span/text()")

        new_item['status'] =  'verkocht'

        # new_item['aanvaarding'] =  self.extract_text(response, "//th[contains(.,'Aanvaarding')]/following-sibling::td[1]/span/text()")
        
        periodic_contribution_vve = self.extract_text(response, "//th[contains(.,'Bijdrage VvE')]/following-sibling::td[1]/span/text()")
        periodic_contribution_periodic = self.extract_text(response, "//th[contains(.,'Periodieke bijdrage')]/following-sibling::td[1]/span/text()")
        periodic_contribution_service = self.extract_text(response, "//th[contains(.,'Servicekosten')]/following-sibling::td[1]/span/text()")
        periodic_contribution = ' '.join([periodic_contribution_vve, periodic_contribution_service, periodic_contribution_periodic]).strip()
        new_item['periodieke_bijdrage_text'] = periodic_contribution

        house_type_detail = self.extract_text(response, "//th[contains(.,'Soort woonhuis')]/following-sibling::td[1]/span/text()")
        appartment_type_detail = self.extract_text(response, "//th[contains(.,'Soort appartement')]/following-sibling::td[1]/span/text()")
        property_type_detail = ' '.join([house_type_detail, appartment_type_detail]).strip()
        new_item['soort_woning'] = property_type_detail

        new_item['soort_bouw'] =  self.extract_text(response, "//th[contains(.,'Bouwvorm')]/following-sibling::td[1]/span/text()")

        new_item['soort_dak'] =  self.extract_text(response, "//th[contains(.,'Soort dak')]/following-sibling::td[1]/span/text()")

        new_item['specifiek'] =  self.extract_text(response, "//th[contains(.,'Specifiek')]/following-sibling::td[1]/span/text()")

        new_item['perceel_oppervlakte_text'] =  self.extract_text(response, "//th[contains(.,'Perceeloppervlakte')]/following-sibling::td[1]/span/text()")

        new_item['inpandige_ruimte_text'] =  self.extract_text(response, "//th[contains(.,'Overige inpandige ruimte')]/following-sibling::td[1]/span/text()")

        new_item['buitenruimte_text'] =  self.extract_text(response, "//th[contains(.,'Gebouwgebonden buitenruimte')]/following-sibling::td[1]/span/text()")



        new_item['inhoud_text'] =  self.extract_text(response, "//th[contains(.,'Inhoud')]/following-sibling::td[1]/span/text()")

        new_item['woonlagen_text'] =  self.extract_text(response, "//th[contains(.,'Aantal woonlagen')]/following-sibling::td[1]/span/text()")

        new_item['badkamers_text'] =  self.extract_text(response, "//th[contains(.,'Aantal badkamers')]/following-sibling::td[1]/span/text()")

        new_item['gelegen_op_text'] =  self.extract_text(response, "//th[contains(.,'Gelegen op')]/following-sibling::td[1]/span/text()")

        new_item['badkamervoorzieningen'] =  self.extract_text(response, "//th[contains(.,'Badkamervoorzieningen')]/following-sibling::td[1]/span/text()")

        new_item['externe_bergruimte_text'] =  self.extract_text(response, "//th[contains(.,'Externe bergruimte')]/following-sibling::td[1]/span/text()")

        new_item['voorzieningen'] =  self.extract_text(response, "//th[contains(.,'Voorzieningen')]/following-sibling::td[1]/span/text()")

        new_item['energielabel_text'] =  self.extract_text(response, "//span[contains(@class, 'energielabel')]/text()")

        new_item['isolatie'] =  self.extract_text(response, "//th[contains(.,'Isolatie')]/following-sibling::td[1]/span/text()")
        
        new_item['verwarming'] =  self.extract_text(response, "//th[contains(.,'Verwarming')]/following-sibling::td[1]/span/text()")

        new_item['warm_water'] =  self.extract_text(response, "//th[contains(.,'Warm water')]/following-sibling::td[1]/span/text()")

        new_item['cv_ketel'] =  self.extract_text(response, "//th[contains(.,'Cv-ketel')]/following-sibling::td[1]/span/text()")

        new_item['eigendomssituatie_text'] =  self.extract_text(response, "//th[contains(.,'Eigendomssituatie')]/following-sibling::td[1]/span/text()")

        new_item['lasten_text'] =  self.extract_text(response, "//th[contains(.,'Lasten')]/following-sibling::td[1]/span/text()")

        new_item['ligging'] =  self.extract_text(response, "//th[contains(.,'Ligging')]/following-sibling::td[1]/span/text()")

        new_item['tuin_text'] =  self.extract_text(response, "//th[contains(.,'Tuin')]/following-sibling::td[1]/span/text()")

        new_item['achtertuin_text'] =  self.extract_text(response, "//th[contains(.,'Achtertuin')]/following-sibling::td[1]/span/text()")

        new_item['voortuin_text'] =  self.extract_text(response, "//th[contains(.,'Voortuin')]/following-sibling::td[1]/span/text()")

        new_item['ligging_tuin_text'] =  self.extract_text(response, "//th[contains(.,'Ligging tuin')]/following-sibling::td[1]/span/text()")

        new_item['balkon_of_dakterras'] =  self.extract_text(response, "//th[contains(.,'Balkon / dakterras')]/following-sibling::td[1]/span/text()")

        new_item['schuur_of_berging'] =  self.extract_text(response, "//th[contains(.,'Schuur/berging')]/following-sibling::td[1]/span/text()")

        new_item['garage'] =  self.extract_text(response, "//th[contains(.,'Garage')]/following-sibling::td[1]/span/text()")

        new_item['garage_capaciteit_text'] =  self.extract_text(response, "//th[contains(.,'Capaciteit')]/following-sibling::td[1]/span/text()")

        new_item['parkeergelegenheid'] =  self.extract_text(response, "//th[contains(.,'Soort parkeergelegenheid')]/following-sibling::td[1]/span/text()")


        # year_built_td = response.xpath("///th[contains(.,'Bouwjaar')]/following-sibling::td[1]/span/text()").extract()[0]
        # year_built = re.findall(r'\d{4}',year_built_td)[0]
        # area_td = response.xpath("///th[contains(.,'woonoppervlakte')]/following-sibling::td[1]/span/text()").extract()[0]
        # area = re.findall(r'\d+',area_td)[0]
        # rooms_td = response.xpath("///th[contains(.,'Aantal kamers')]/following-sibling::td[1]/span/text()").extract()[0]
        # rooms = re.findall('\d+ kamer',rooms_td)[0].replace(' kamer','')
        # bedrooms = re.findall('\d+ slaapkamer',rooms_td)[0].replace(' slaapkamer','')

        # new_item['year_built'] = year_built
        # new_item['area'] = area
        # new_item['rooms'] = rooms
        # new_item['bedrooms'] = bedrooms

        yield new_item

    def extract_text(self, response, xpath):
        results = response.xpath(xpath).extract()
        return results[0].strip().lower() if results else ''
