# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.item.org/en/latest/topics/item-pipeline.html

from azure.storage.table import TableService, Entity

table_service = TableService(account_name='mlhousing', account_key='RtqHj1/pRK+2WsMjZuql7TbyXOQwk4DRXJ/iLLrShwA8/9uTzxTuqomYaq4IW0szQ6JIdKVAANapJkOge/aGEQ==')

class FundaPipeline(object):
    def process_item(self, item, spider):
        
        house = {
            'PartitionKey': dict(item).get('city', 'unknown_city'),
            'RowKey': dict(item).get('address', 'unknown_address').replace("  ", "_"),
            'url' :  dict(item).get('url', ''),
            'title' : dict(item).get('title', ''),
            'address' : dict(item).get('address', ''),
            'postal_code' : dict(item).get('postal_code', ''),
            'price' : dict(item).get('price', ''),              # Listing price ("Vraagprijs")
            'year_built' : dict(item).get('year_built', ''),         # Year built ("Bouwjaar")
            'area' :  dict(item).get('area', ''),               # Built area ("Woonoppervlakte")
            'rooms':  dict(item).get('rooms', ''),              # Number of rooms
            'bedrooms' : dict(item).get('bedrooms', ''),           # Number of bedrooms
            'property_type' : dict(item).get('property_type', ''),      # House or apartment
            'city' : dict(item).get('city', ''),
            'posting_date' : dict(item).get('posting_date', ''),
            'sale_date' : dict(item).get('sale_date', '')
        }
        table_service.insert_entity('HousesForSale', house)        
        return item


