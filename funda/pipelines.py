# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.item.org/en/latest/topics/item-pipeline.html

from azure.storage.table import TableService, Entity
import datetime
import re

table_service = TableService(account_name='mlhousing', account_key='RtqHj1/pRK+2WsMjZuql7TbyXOQwk4DRXJ/iLLrShwA8/9uTzxTuqomYaq4IW0szQ6JIdKVAANapJkOge/aGEQ==')

class PreprocessPipeline(object):
    
    def process_item(self, item, spider):
        # woningtype
        if re.search(r'/appartement-',item['url']):
            item['woningtype'] = "appartement"
        elif re.search(r'/huis-',item['url']):
            item['woningtype'] = "huis"
            
        # address info
        item['postcode'] = re.search(r'\d{4} [A-Z]{2}', item['title']).group(0)
        item['gemeente'] = re.search(r'\d{4} [A-Z]{2} \w+', item['title']).group(0).split()[2]
        item['address'] = re.findall(r'te koop: (.*) \d{4}', item['title'])[0]
        item['straat'] = re.findall(r'te koop: ([a-zA-Z\. -]*) ', item['title'])[0]
        item['huisnummer'] = re.findall(r'\d+', item['title'])[0]
        
        # vraagprijs
        price = re.findall(r' \d+.\d+', item['vraagprijs_text'])[0].strip().replace('.','')
        item['vraagprijs'] = price
        
        # bouwjaar
        year_built = re.findall(r'\d+', item['bouwjaar_text'])[0] if item['bouwjaar_text'] else ''
        item['bouwjaar'] = year_built

        # woonoppervlakte
        area = re.findall(r'\d+', item['woonoppervlakte_text'])[0] if item['woonoppervlakte_text'] else ''
        item['woonoppervlakte'] = area

        # kamers
        rooms = re.findall('\d+ kamer',item['kamers_text'])
        rooms = rooms[0].replace(' kamer','') if rooms else ''
        item['kamers'] = rooms
        bedrooms = re.findall('\d+ slaapkamer',item['kamers_text'])
        bedrooms = bedrooms[0].replace(' slaapkamer','') if bedrooms else ''
        item['slaapkamers'] = bedrooms

            
            
            
        return item

class StoragePipeline(object):
    def process_item(self, item, spider):
        
        house = {
            'PartitionKey': dict(item).get('gemeente', 'aaa'),
            'RowKey': dict(item).get('address', 'aaa').replace("  ", "_"),
            'url' :  dict(item).get('url', ''),
            'address' : dict(item).get('address', ''),
            'postcode' : dict(item).get('postcode', ''),
            'vraagprijs' : dict(item).get('vraagprijs', ''),
            'bouwjaar' : dict(item).get('bouwjaar', ''),
            'woonoppervlakte' :  dict(item).get('woonoppervlakte', ''),
            'kamers':  dict(item).get('kamers', ''),
            'slaapkamers' : dict(item).get('slaapkamers', ''),
            'woningtype' : dict(item).get('woningtype', ''),
            'gemeente' : dict(item).get('gemeente', ''),

            'periodieke_bijdrage_text' : dict(item).get('periodieke_bijdrage', ''),
            'soort_woning' : dict(item).get('soort_woning', ''),
            'soort_bouw' : dict(item).get('soort_bouw', ''),
            'perceel_oppervlakte' : dict(item).get('perceel_oppervlakte', ''),
            'soort_dak' : dict(item).get('soort_dak', ''),
            'specifiek' : dict(item).get('specifiek', ''),
            'inpandige_ruimte' : dict(item).get('inpandige_ruimte', ''),
            'buitenruimte' : dict(item).get('buiten_ruimte', ''),
            'status' : dict(item).get('status', ''),
            'aanvaarding' : dict(item).get('aanvaarding', ''),
            
#            'aanbod_jaar' : datetime.datetime.now().year,
#            'aanbod_maand' : datetime.datetime.now().month,
#            'sale_date' : dict(item).get('sale_date', '')
        }
        table_service.insert_entity('HousesForSale', house)        
        return item


