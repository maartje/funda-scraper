# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.item.org/en/latest/topics/item-pipeline.html

from azure.storage.table import TableService, Entity
import datetime
import re
from scrapy.exceptions import DropItem

table_service = TableService(account_name='mlhousing', account_key='RtqHj1/pRK+2WsMjZuql7TbyXOQwk4DRXJ/iLLrShwA8/9uTzxTuqomYaq4IW0szQ6JIdKVAANapJkOge/aGEQ==')


def try_extract_number(item, item_key, regex = r'[\d.]+'):
    text = try_extract(item, item_key, regex)
    if not(text):
        return None
    return int(text.replace('.', ''))

def try_extract_date(item, item_key, regex = r'\d\d?-\d\d?-\d{4}'):
    text = try_extract(item, item_key, regex)
    if not(text):
        return None
    return datetime.datetime.strptime(text, "%d-%m-%Y")

def try_extract(item, item_key, regex):
    text  = item.get(item_key, '')
    matches = re.findall(regex, text, re.IGNORECASE)
    if not(matches):
        return None
    return matches[0]

class PreprocessPipeline(object):



    def process_item(self, item, spider):
        if not(item.get('url')):
            raise DropItem("Missing url in %s" % item)
        
        # woningtype
        item['woningtype'] = try_extract(item, 'url', r'/(appartement)-') or try_extract(item, 'url', r'/(huis)-')

        # address info
        item['postcode'] = try_extract(item, 'title', r'\d{4} [A-Z]{2}')
        item['postcode_regio'] = try_extract(item, 'title', r'(\d{2})\d{2} [A-Z]{2}')
        item['postcode_wijk'] = try_extract(item, 'title', r'(\d{4}) [A-Z]{2}')
        item['gemeente'] = try_extract(item, 'title', r'\d{4} [A-Z]{2} (\w+)')
        item['straat'] = try_extract(item, 'title', r'te koop: ([a-zA-Z\. -]*) \d+') or try_extract(item, 'title', r'Verkocht: ([a-zA-Z\.-]*) ')
        item['huisnummer'] = try_extract(item, 'title', r'\d+')
        
        item['energielabel'] = try_extract(item, 'energielabel_text', r'[a-zA-Z]') 

        # vraagprijs
        item['vraagprijs'] = try_extract_number(item, 'vraagprijs_text', r'[\d.]+')
        item['kosten_koper'] = not(try_extract(item, 'vraagprijs_text', r'v\.o\.n'))

        # woonoppervlakte
        item['woonoppervlakte'] = try_extract_number(item, 'woonoppervlakte_text', r'[\d.]+')

        item['aangeboden_sinds'] =  try_extract_date(item, 'aangeboden_sinds_text') 

        item['verkoopdatum'] =  try_extract_date(item, 'verkoopdatum_text') 

        # bouwjaar
        item['bouwjaar'] =  try_extract_number(item, 'bouwjaar_text', r'\d{4}') 

        # bouwperiode
        item['bouwperiode_start'] = try_extract_number(item, 'bouwperiode_text', r'(\d{4})-\d{4}') or item.get('bouwjaar') 
        item['bouwperiode_end'] = try_extract_number(item, 'bouwperiode_text', r'\d{4}-(\d{4})') or item.get('bouwjaar')

        # inhoud
        item['inhoud'] = try_extract_number(item, 'inhoud_text', r'[\d.]+')

        # perceel_oppervlakte
        item['perceel_oppervlakte'] = try_extract_number(item, 'perceel_oppervlakte_text', r'[\d.]+')

        # inpandige ruimte
        item['inpandige_ruimte'] = try_extract_number(item, 'inpandige_ruimte_text', r'[\d.]+')

        item['externe_bergruimte'] = try_extract_number(item, 'externe_bergruimte_text', r'[\d.]+')

        # buitenruimte
        item['buitenruimte'] = try_extract_number(item, 'buitenruimte_text', r'[\d.]+')

        periodieke_bijdrage = try_extract_number(item, 'periodieke_bijdrage_text', r'([\d.]+),?\d* per maand')
        vve_bijdrage = try_extract_number(item, 'vve_bijdrage_text', r'([\d.]+),?\d* per maand')
        service_kosten = try_extract_number(item, 'service_kosten_text', r'([\d.]+),?\d* per maand')
        item['periodieke_bijdrage'] = periodieke_bijdrage or vve_bijdrage or service_kosten
        
        # kamers
        item['kamers'] = try_extract_number(item, 'kamers_text', r'(\d+) kamer')
        item['slaapkamers'] = try_extract_number(item, 'kamers_text', r'(\d+) slaapkamer')

        #badkamers en toiletten
        item['badkamers'] = try_extract_number(item, 'badkamers_text', r'(\d+) badkamer')
        item['toiletten'] = try_extract_number(item, 'badkamers_text', r'(\d+) apart')

        #balkon/dakterras    
        balkon_of_dakterras = item.get('balkon_of_dakterras', '').lower()
        item['frans_balkon'] = 'frans balkon' in balkon_of_dakterras
        item['dakterras'] = 'dakterras' in balkon_of_dakterras
        item['balkon'] = 'balkon' in balkon_of_dakterras.replace('frans balkon', '')
        
        # garage capaciteit
        item['garage_capaciteit'] = try_extract_number(item, 'garage_capaciteit_text', r'(\d+) auto')

        # achtertuin 1.088 m² (8m diep en 11m breed)
        item['achtertuin_diepte'] = try_extract_number(item, 'achtertuin_text', r'(\d+)m diep')
        item['achtertuin_breedte'] = try_extract_number(item, 'achtertuin_text', r'(\d+)m breed')
        item['achtertuin_oppervlakte'] = try_extract_number(item, 'achtertuin_text', r'([\d.]+) m')

        # voortuin
        item['voortuin_diepte'] = try_extract_number(item, 'voortuin_text', r'(\d+)m diep')
        item['voortuin_breedte'] = try_extract_number(item, 'voortuin_text', r'(\d+)m breed')
        item['voortuin_oppervlakte'] = try_extract_number(item, 'voortuin_text', r'([\d.]+) m')

        # zijtuin
        item['zijtuin_diepte'] = try_extract_number(item, 'zijtuin_text', r'(\d+)m diep')
        item['zijtuin_breedte'] = try_extract_number(item, 'zijtuin_text', r'(\d+)m breed')
        item['zijtuin_oppervlakte'] = try_extract_number(item, 'zijtuin_text', r'([\d.]+) m')

        # zonneterras
        item['zonneterras_diepte'] = try_extract_number(item, 'zonneterras_text', r'(\d+)m diep')
        item['zonneterras_breedte'] = try_extract_number(item, 'zonneterras_text', r'(\d+)m breed')
        item['zonneterras_oppervlakte'] = try_extract_number(item, 'zonneterras_text', r'([\d.]+) m')

        # patio
        item['patio_diepte'] = try_extract_number(item, 'patio_text', r'(\d+)m diep')
        item['patio_breedte'] = try_extract_number(item, 'patio_text', r'(\d+)m breed')
        item['patio_oppervlakte'] = try_extract_number(item, 'patio_text', r'([\d.]+) m')

        # tuin_text
        tuin_text = item.get('tuin_text', '').lower()
        item['achtertuin'] = len(item.get('achtertuin_text', '').strip()) > 0 or 'achtertuin' in tuin_text
        item['voortuin'] = len(item.get('voortuin_text', '').strip()) > 0 or 'voortuin' in tuin_text
        item['patio'] = len(item.get('patio_text', '').strip()) > 0 or 'patio' in tuin_text
        item['zonneterras'] = len(item.get('zonneterras_text', '').strip()) > 0 or 'zonneterras' in tuin_text
        item['zijtuin'] = len(item.get('zijtuin_text', '').strip()) > 0 or 'zijtuin' in tuin_text
        item['tuin_rondom'] = 'rondom' in tuin_text
        item['plaats'] = 'plaats' in tuin_text

        # ligging_tuin
        ligging_tuin_text = item.get('ligging_tuin_text', '').lower()
        if "zuidoost" in ligging_tuin_text: item['ligging_tuin'] = 'ZO' 
        elif "zuidwest" in ligging_tuin_text: item['ligging_tuin'] = 'ZW' 
        elif "noordoost" in ligging_tuin_text: item['ligging_tuin'] = 'NO' 
        elif "noordwest" in ligging_tuin_text: item['ligging_tuin'] = 'NW' 
        elif "noorden" in ligging_tuin_text: item['ligging_tuin'] = 'N' 
        elif "westen" in ligging_tuin_text: item['ligging_tuin'] = 'W' 
        elif "oosten" in ligging_tuin_text: item['ligging_tuin'] = 'O' 
        elif "zuiden" in ligging_tuin_text: item['ligging_tuin'] = 'Z' 
        
        item['achterom'] =  "achterom" in ligging_tuin_text
        

        item['soort_woning'] = item.get('soort_huis') or item.get('soort_appartement')
        
        item['woonlagen'] = try_extract_number(item, 'woonlagen_text', r'(\d+) woonla')
        woonlagen_text = item.get('woonlagen_text', '').lower()
        item['kelder'] = "kelder" in woonlagen_text
        item['vliering'] = "vliering" in woonlagen_text
        item['zolder'] = "zolder" in woonlagen_text
    
    
        item['verdieping'] = 0 if "begane grond" in item.get('gelegen_op_text', '') else try_extract_number(item, 'gelegen_op_text', r'\d+')

        
        eigendoms_info = (item.get("eigendomssituatie_text", '') + item.get("lasten_text", '')).lower()
        volle_eigendom = "volle eigendom" in eigendoms_info
        erfpacht = "erfpacht" in eigendoms_info
        afgekocht = erfpacht and ("afgekocht" in eigendoms_info)

        if erfpacht:
            item['eigendomssituatie'] = 'erfpacht'
        elif volle_eigendom:
            item['eigendomssituatie'] = 'volle eigendom'
        else:
            item['eigendomssituatie'] = None
            

        item['eind_datum_erfpacht'] = try_extract_date(item, "eigendomssituatie_text") or try_extract_date(item, "lasten_text")

        kosten_erfpacht_match = re.findall(r'([\d.]+),?\d* per jaar', eigendoms_info) 
        if afgekocht or volle_eigendom: 
            item["kosten_erfpacht"] = 0
        elif kosten_erfpacht_match: 
            item["kosten_erfpacht"] = int(kosten_erfpacht_match[0].replace('.', ''))  
        elif 'einddatum' in eigendoms_info:
            item["kosten_erfpacht"] = 0 
        else: 
            item["kosten_erfpacht"] = None
        
        return item

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
            'looptijd_text':  dict(item).get('looptijd_text', ''),
            'toegankelijkheid_text':  dict(item).get('toegankelijkheid_text', ''),
            'keurmerken_text':  dict(item).get('keurmerken_text', ''),
            
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
            'garage' : dict(item).get('garage', ''),
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
            'inpandige_ruimte' : dict(item).get('inpandige_ruimte', ''),
            'buitenruimte' : dict(item).get('buitenruimte', ''),
            
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
            'externe_bergruimte' : dict(item).get('externe_bergruimte', ''),
            'verdieping' : dict(item).get('verdieping', ''),
            'inhoud' : dict(item).get('inhoud', ''),
            
            'eigendomssituatie' : dict(item).get('eigendomssituatie', ''),
            'eind_datum_erfpacht' : dict(item).get('eind_datum_erfpacht', ''),
            'kosten_erfpacht' : dict(item).get('kosten_erfpacht', ''),

            'energielabel' : dict(item).get('energielabel', ''),
        }
        # table_service.insert_or_replace_entity('HousesForSale', house)        
        # table_service.insert_or_replace_entity('HousesSold', house)        
        return item

