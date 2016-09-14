# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.item.org/en/latest/topics/item-pipeline.html

import datetime
import re
from scrapy.exceptions import DropItem

def try_extract_integer(item, item_key, regex = r'[\d.]+'):
    text = try_extract_string(item, item_key, regex)
    if not(text):
        return None
    return int(text.replace('.', '').replace(',', '.'))

def try_extract_double(item, item_key, regex = r'[\d.,]+'):
    text = try_extract_string(item, item_key, regex)
    if not(text):
        return None
    return float(text.replace('.', '').replace(',', '.'))

def try_extract_date(item, item_key, regex = r'\d\d?-\d\d?-\d{4}'):
    text = try_extract_string(item, item_key, regex)
    if not(text):
        return None
    return datetime.datetime.strptime(text, "%d-%m-%Y")

def try_extract_string(item, item_key, regex, flags = re.IGNORECASE):
    text  = item.get(item_key, '')
    matches = re.findall(regex, text, flags)
    if not(matches):
        return None
    return matches[0]

class PreprocessPipeline(object):



    def process_item(self, item, spider):
        if not(item.get('url')):
            raise DropItem("Missing url in %s" % item)

        item['insertion_date'] = datetime.datetime.now()
        
        # woningtype
        item['woningtype'] = try_extract_string(item, 'url', r'/(appartement)-') or try_extract_string(item, 'url', r'/(huis)-')
        
        # funda_id
        item['funda_id'] = try_extract_string(item, 'url', r'-(\d+)-') 
        
        item['verkocht'] = 'verkocht' in item.get('url')

        # address info
        item['postcode'] = try_extract_string(item, 'title', r'\d{4} [A-Z]{2}', 0)
        item['postcode_regio'] = try_extract_string(item, 'title', r'(\d{2})\d{2} [A-Z]{2}', 0)
        item['postcode_wijk'] = try_extract_string(item, 'title', r'(\d{4}) [A-Z]{2}', 0)
        item['gemeente'] = try_extract_string(item, 'title', r'\d{4} [A-Z]{2} (\w+)', 0)
        item['straat'] = try_extract_string(item, 'title', r'te koop: ([a-zA-Z\. -]*) \d+') or try_extract_string(item, 'title', r'Verkocht: ([a-zA-Z\.-]*) ')
        item['huisnummer'] = try_extract_string(item, 'title', r'\d+')
        
        item['id'] = item['postcode_wijk'] + '_'+ item['huisnummer'] + '_'+ item['funda_id']

        item['energielabel'] = try_extract_string(item, 'energielabel_text', r'[a-zA-Z]') 

        # vraagprijs
        item['vraagprijs'] = try_extract_double(item, 'vraagprijs_text', r'[\d.]+')
        item['kosten_koper'] = not(try_extract_string(item, 'vraagprijs_text', r'v\.o\.n'))

        # woonoppervlakte
        item['woonoppervlakte'] = try_extract_double(item, 'woonoppervlakte_text', r'[\d.]+')

        item['aangeboden_sinds'] =  try_extract_date(item, 'aangeboden_sinds_text') 

        item['verkoopdatum'] =  try_extract_date(item, 'verkoopdatum_text') 

        # bouwjaar
        item['bouwjaar'] =  try_extract_integer(item, 'bouwjaar_text', r'\d{4}') 

        # bouwperiode
        item['bouwperiode_start'] = try_extract_integer(item, 'bouwperiode_text', r'(\d{4})-\d{4}') or item.get('bouwjaar')
        item['bouwperiode_end'] = try_extract_integer(item, 'bouwperiode_text', r'\d{4}-(\d{4})') or item.get('bouwjaar')

        # inhoud
        item['inhoud'] = try_extract_double(item, 'inhoud_text', r'[\d.]+')

        # perceel_oppervlakte
        item['perceel_oppervlakte'] = try_extract_double(item, 'perceel_oppervlakte_text', r'[\d.]+')

        # inpandige ruimte
        item['inpandige_ruimte_oppervlakte'] = try_extract_double(item, 'inpandige_ruimte_text', r'[\d.]+')

        item['externe_bergruimte_oppervlakte'] = try_extract_double(item, 'externe_bergruimte_text', r'[\d.]+')

        # buitenruimte_oppervlakte
        item['buitenruimte_oppervlakte'] = try_extract_double(item, 'buitenruimte_text', r'[\d.]+')

        periodieke_bijdrage = try_extract_double(item, 'periodieke_bijdrage_text', r'([\d.,]+) per maand')
        vve_bijdrage = try_extract_double(item, 'vve_bijdrage_text', r'([\d.,]+) per maand')
        service_kosten = try_extract_double(item, 'service_kosten_text', r'([\d.,]+) per maand')
        item['periodieke_bijdrage'] = periodieke_bijdrage or vve_bijdrage or service_kosten
        
        # kamers
        item['kamers'] = try_extract_integer(item, 'kamers_text', r'(\d+) kamer')
        item['slaapkamers'] = try_extract_integer(item, 'kamers_text', r'(\d+) slaapkamer')

        #badkamers en toiletten
        item['badkamers'] = try_extract_integer(item, 'badkamers_text', r'(\d+) badkamer')
        item['aparte_toiletten'] = try_extract_integer(item, 'badkamers_text', r'(\d+) apart')

        #balkon/dakterras    
        balkon_of_dakterras = item.get('balkon_of_dakterras', '').lower()
        item['frans_balkon'] = 'frans balkon' in balkon_of_dakterras
        item['dakterras'] = 'dakterras' in balkon_of_dakterras
        item['balkon'] = 'balkon' in balkon_of_dakterras.replace('frans balkon', '')
        
        # garage capaciteit
        item['garage_capaciteit'] = try_extract_integer(item, 'garage_capaciteit_text', r'(\d+) auto')

        # achtertuin 1.088 m² (8m diep en 11m breed)
        item['achtertuin_diepte'] = try_extract_double(item, 'achtertuin_text', r'([\d,]+)m diep')
        item['achtertuin_breedte'] = try_extract_double(item, 'achtertuin_text', r'([\d,]+)m breed')
        item['achtertuin_oppervlakte'] = try_extract_double(item, 'achtertuin_text', r'([\d.,]+) m')

        # voortuin
        item['voortuin_diepte'] = try_extract_double(item, 'voortuin_text', r'([\d,]+)m diep')
        item['voortuin_breedte'] = try_extract_double(item, 'voortuin_text', r'([\d,]+)m breed')
        item['voortuin_oppervlakte'] = try_extract_double(item, 'voortuin_text', r'([\d.,]+) m')

        # zijtuin
        item['zijtuin_diepte'] = try_extract_double(item, 'zijtuin_text', r'([\d,]+)m diep')
        item['zijtuin_breedte'] = try_extract_double(item, 'zijtuin_text', r'([\d,]+)m breed')
        item['zijtuin_oppervlakte'] = try_extract_double(item, 'zijtuin_text', r'([\d.,]+) m')

        # zonneterras
        item['zonneterras_diepte'] = try_extract_double(item, 'zonneterras_text', r'([\d,]+)m diep')
        item['zonneterras_breedte'] = try_extract_double(item, 'zonneterras_text', r'([\d,]+)m breed')
        item['zonneterras_oppervlakte'] = try_extract_double(item, 'zonneterras_text', r'([\d.,]+) m')

        # patio
        item['patio_diepte'] = try_extract_double(item, 'patio_text', r'([\d,]+)m diep')
        item['patio_breedte'] = try_extract_double(item, 'patio_text', r'([\d,]+)m breed')
        item['patio_oppervlakte'] = try_extract_double(item, 'patio_text', r'([\d.,]+) m')

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
        
        item['woonlagen'] = try_extract_integer(item, 'woonlagen_text', r'(\d+) woonla')
        woonlagen_text = item.get('woonlagen_text', '').lower()
        item['kelder'] = "kelder" in woonlagen_text
        item['vliering'] = "vliering" in woonlagen_text
        item['zolder'] = "zolder" in woonlagen_text
    
    
        item['verdieping'] = 0 if "begane grond" in item.get('gelegen_op_text', '') else try_extract_integer(item, 'gelegen_op_text', r'\d+')

        
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


