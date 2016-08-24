# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.item.org/en/latest/topics/item-pipeline.html

from azure.storage.table import TableService, Entity
import datetime

table_service = TableService(account_name='mlhousing', account_key='RtqHj1/pRK+2WsMjZuql7TbyXOQwk4DRXJ/iLLrShwA8/9uTzxTuqomYaq4IW0szQ6JIdKVAANapJkOge/aGEQ==')

class FundaPipeline(object):
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

            'periodieke_bijdrage' : dict(item).get('periodieke_bijdrage', ''),
            'soort_woning' : dict(item).get('soort_woning', ''),
            'soort_bouw' : dict(item).get('soort_bouw', ''),
            'perceel_oppervlakte' : dict(item).get('perceel_oppervlakte', ''),
            'soort_dak' : dict(item).get('soort_dak', ''),
            'specifiek' : dict(item).get('specifiek', ''),
            'inpandige_ruimte' : dict(item).get('inpandige_ruimte', ''),
            'buitenruimte' : dict(item).get('buiten_ruimte', ''),
            
#            'aanbod_jaar' : datetime.datetime.now().year,
#            'aanbod_maand' : datetime.datetime.now().month,
#            'sale_date' : dict(item).get('sale_date', '')
        }
        table_service.insert_entity('HousesForSale', house)        
        return item


