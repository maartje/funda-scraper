# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.item.org/en/latest/topics/item-pipeline.html

from azure.storage.table import TableService, Entity
from scrapy.exceptions import DropItem

table_service = TableService(account_name='mlhousing', account_key='RtqHj1/pRK+2WsMjZuql7TbyXOQwk4DRXJ/iLLrShwA8/9uTzxTuqomYaq4IW0szQ6JIdKVAANapJkOge/aGEQ==')

class StoragePipeline(object):
    def process_item(self, item, spider):
        
        # check required fields (used in partition and row key)
        if not(item.get('gemeente')): #Todo: use postcode regio?
            raise DropItem("Missing 'gemeente' in %s" % item.get('url', item))
        if not(item.get('postcode')):
            raise DropItem("Missing 'postcode' in %s" % item.get('url', item))
        if not(item.get('huisnummer')):
            raise DropItem("Missing 'huisnummer' in %s" % item.get('url', item))
            
        house = {
            'PartitionKey': item['gemeente'],
            'RowKey': item['postcode'] + " " + item['huisnummer'],
            
            'url' :  dict(item).get('url', ''),
            'title' :  dict(item).get('title', ''),
            'vraagprijs_text' : dict(item).get('vraagprijs_text', ''),
            'periodieke_bijdrage_text' : dict(item).get('periodieke_bijdrage_text', ''),
            'vve_bijdrage_text' : dict(item).get('vve_bijdrage_text', ''),
            'service_kosten_text' : dict(item).get('service_kosten_text', ''),
            'bouwjaar_text' : dict(item).get('bouwjaar_text', ''),
            'bouwperiode_text' : dict(item).get('bouwperiode_text', ''),
            'woonoppervlakte_text' :  dict(item).get('woonoppervlakte_text', ''),
            'kamers_text':  dict(item).get('kamers_text', ''),
            'aangeboden_sinds_text':  dict(item).get('aangeboden_sinds_text', ''),
            'verkoopdatum_text':  dict(item).get('verkoopdatum_text', ''),
            'looptijd':  dict(item).get('looptijd', ''),
            'toegankelijkheid':  dict(item).get('toegankelijkheid', ''),
            'keurmerken':  dict(item).get('keurmerken', ''),
            
            'soort_huis' : dict(item).get('soort_huis', ''),
            'soort_appartement' : dict(item).get('soort_appartement', ''),
            'soort_bouw' : dict(item).get('soort_bouw', ''),
            'soort_dak' : dict(item).get('soort_dak', ''),
            'specifiek' : dict(item).get('specifiek', ''),
            'perceel_oppervlakte_text' : dict(item).get('perceel_oppervlakte_text', ''),
            'inpandige_ruimte_text' : dict(item).get('inpandige_ruimte_text', ''),
            'buitenruimte_text' : dict(item).get('buitenruimte_text', ''),
            'status' : dict(item).get('status', ''),
            'aanvaarding' : dict(item).get('aanvaarding', ''),

            'inhoud_text' : dict(item).get('inhoud_text', ''),
            'woonlagen_text' : dict(item).get('woonlagen_text', ''),
            'badkamers_text' : dict(item).get('badkamers_text', ''),
            'gelegen_op_text' : dict(item).get('gelegen_op_text', ''),
            'badkamervoorzieningen' : dict(item).get('badkamervoorzieningen', ''),
            'externe_bergruimte_text' : dict(item).get('externe_bergruimte_text', ''),
            'voorzieningen' : dict(item).get('voorzieningen', ''),
            'energielabel_text' : dict(item).get('energielabel_text', ''),
            'isolatie' : dict(item).get('isolatie', ''),
            'verwarming' : dict(item).get('verwarming', ''),
            'warm_water' : dict(item).get('warm_water', ''),
            'cv_ketel' : dict(item).get('cv_ketel', ''),
            'eigendomssituatie_text' : dict(item).get('eigendomssituatie_text', ''),
            'lasten_text' : dict(item).get('lasten_text', ''),
            'ligging' : dict(item).get('ligging', ''),
            'tuin_text' : dict(item).get('tuin_text', ''),
            'achtertuin_text' : dict(item).get('achtertuin_text', ''),
            'voortuin_text' : dict(item).get('voortuin_text', ''),
            'zijtuin_text' : dict(item).get('zijtuin_text', ''),
            'patio_text' : dict(item).get('patio_text', ''),
            'zonneterras_text' : dict(item).get('zonneterras_text', ''),
            'ligging_tuin_text' : dict(item).get('ligging_tuin_text', ''),
            'balkon_of_dakterras' : dict(item).get('balkon_of_dakterras', ''),
            'schuur_of_berging' : dict(item).get('schuur_of_berging', ''),
            'garage_text' : dict(item).get('garage_text', ''),
            'garage_capaciteit_text' : dict(item).get('garage_capaciteit_text', ''),
            'parkeergelegenheid' : dict(item).get('parkeergelegenheid', ''),
            
            
            # derived values

            'woningtype' : dict(item).get('woningtype', ''),
            'soort_woning' : dict(item).get('soort_woning', ''),
            'gemeente' : dict(item).get('gemeente', ''),
            'postcode' : dict(item).get('postcode', ''),
            'postcode_regio' : dict(item).get('postcode_regio', ''),
            'postcode_wijk' : dict(item).get('postcode_wijk', ''),
            'straat' : dict(item).get('straat', ''),
            'huisnummer' : dict(item).get('huisnummer', ''),

            'vraagprijs' : dict(item).get('vraagprijs', ''),
            'kosten_koper' : dict(item).get('kosten_koper', ''),

            'aangeboden_sinds' : dict(item).get('aangeboden_sinds', ''),
            'verkoopdatum' : dict(item).get('verkoopdatum', ''),

            'bouwjaar' : dict(item).get('bouwjaar', ''),
            'bouwperiode_start' : dict(item).get('bouwperiode_start', ''),
            'bouwperiode_end' : dict(item).get('bouwperiode_end', ''),
            
            
            'woonoppervlakte' :  dict(item).get('woonoppervlakte', ''),
            'perceel_oppervlakte' : dict(item).get('perceel_oppervlakte', ''),
            'inpandige_ruimte_oppervlakte' : dict(item).get('inpandige_ruimte_oppervlakte', ''),
            'buitenruimte_oppervlakte' : dict(item).get('buitenruimte_oppervlakte', ''),
            
            'kamers':  dict(item).get('kamers', ''),
            'slaapkamers' : dict(item).get('slaapkamers', ''),

            'badkamers':  dict(item).get('badkamers', ''),
            'toiletten':  dict(item).get('toiletten', ''),

            'periodieke_bijdrage' : dict(item).get('periodieke_bijdrage', ''),

            'frans_balkon' : dict(item).get('frans_balkon', ''),
            'dakterras' : dict(item).get('dakterras', ''),
            'balkon' : dict(item).get('balkon', ''),
            
            'voortuin_diepte' : dict(item).get('voortuin_diepte', ''),
            'voortuin_breedte' : dict(item).get('voortuin_breedte', ''),
            'voortuin_oppervlakte' : dict(item).get('voortuin_oppervlakte', ''),
            
            'achtertuin_diepte' : dict(item).get('achtertuin_diepte', ''),
            'achtertuin_breedte' : dict(item).get('achtertuin_breedte', ''),
            'achtertuin_oppervlakte' : dict(item).get('achtertuin_oppervlakte', ''),

            'patio_diepte' : dict(item).get('patio_diepte', ''),
            'patio_breedte' : dict(item).get('patio_breedte', ''),
            'patio_oppervlakte' : dict(item).get('patio_oppervlakte', ''),

            'zijtuin_diepte' : dict(item).get('zijtuin_diepte', ''),
            'zijtuin_breedte' : dict(item).get('zijtuin_breedte', ''),
            'zijtuin_oppervlakte' : dict(item).get('zijtuin_oppervlakte', ''),

            'zonneterras_diepte' : dict(item).get('zonneterras_diepte', ''),
            'zonneterras_breedte' : dict(item).get('zonneterras_breedte', ''),
            'zonneterras_oppervlakte' : dict(item).get('zonneterras_oppervlakte', ''),


            'ligging_tuin' : dict(item).get('ligging_tuin', ''),
            'achterom' : dict(item).get('achterom', ''),
            'achtertuin' : dict(item).get('achtertuin', ''),
            'voortuin' : dict(item).get('voortuin', ''),
            'zijtuin' : dict(item).get('zijtuin', ''),
            'tuin_rondom' : dict(item).get('tuin_rondom', ''),
            'patio' : dict(item).get('patio', ''),
            'zonneterras' : dict(item).get('zonneterras', ''),
            'plaats' : dict(item).get('plaats', ''),
            'woonlagen' : dict(item).get('woonlagen', ''),
            'kelder' : dict(item).get('kelder', ''),
            'vliering' : dict(item).get('vliering', ''),
            'zolder' : dict(item).get('zolder', ''),
            'garage_capaciteit' : dict(item).get('garage_capaciteit', ''),
            'externe_bergruimte_oppervlakte' : dict(item).get('externe_bergruimte_oppervlakte', ''),
            'verdieping' : dict(item).get('verdieping', ''),
            'inhoud' : dict(item).get('inhoud', ''),
            
            'eigendomssituatie' : dict(item).get('eigendomssituatie', ''),
            'eind_datum_erfpacht' : dict(item).get('eind_datum_erfpacht', ''),
            'kosten_erfpacht' : dict(item).get('kosten_erfpacht', ''),

            'energielabel' : dict(item).get('energielabel', ''),
        }
        # table_service.insert_or_replace_entity('HousesForSale', house)        
        table_service.insert_or_replace_entity('HousesSold', house)        
        return item
