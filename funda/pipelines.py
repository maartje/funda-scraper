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
        postcode = re.search(r'\d{4} [A-Z]{2}', item['title']).group(0)
        item['postcode'] = postcode
        item['postcode_regio'] = postcode[0:2]
        item['postcode_wijk'] = postcode[0:4]
        item['gemeente'] = re.search(r'\d{4} [A-Z]{2} \w+', item['title']).group(0).split()[2]
        straat = re.findall(r'te koop: ([a-zA-Z\.-]*) ', item['title']) + re.findall(r'Verkocht: ([a-zA-Z\.-]*) ', item['title'])
        item['straat'] = straat[0] if straat else (re.findall(r'te koop: ([a-zA-Z\.-]*)', item['title']) + re.findall(r'Verkocht: ([a-zA-Z\.-]*)', item['title']))[0]
        item['huisnummer'] = re.findall(r'\d+', item['title'])[0]
        
        # vraagprijs
        item['vraagprijs'] = int(re.findall(r'\d+', item['vraagprijs_text'].replace('.',''))[0].strip())
        item['kosten_koper'] = not(re.findall(r' v\.o\.n', item['vraagprijs_text']))

        # bouwjaar
        item['bouwjaar'] = int(re.findall(r'\d+', item['bouwjaar_text'])[0]) if item['bouwjaar_text'] else None

        # woonoppervlakte
        item['woonoppervlakte'] = int(re.findall(r'\d+', item['woonoppervlakte_text'].replace('.',''))[0]) if item['woonoppervlakte_text'] else None

        item['inhoud'] = int(re.findall(r'\d+', item['inhoud_text'].replace('.',''))[0]) if item['inhoud_text'] else None

        # perceel_oppervlakte
        item['perceel_oppervlakte'] = int(re.findall(r'\d+', item['perceel_oppervlakte_text'].replace('.',''))[0]) if item['perceel_oppervlakte_text'] else None
        
        # inpandige ruimte
        item['inpandige_ruimte'] = int(re.findall(r'\d+', item['inpandige_ruimte_text'].replace('.',''))[0]) if item['inpandige_ruimte_text'] else None

        item['externe_bergruimte'] = int(re.findall(r'\d+', item['externe_bergruimte_text'].replace('.',''))[0]) if item['externe_bergruimte_text'] else None

        # buitenruimte
        item['buitenruimte'] = int(re.findall(r'\d+', item['buitenruimte_text'].replace('.',''))[0]) if item['buitenruimte_text'] else None

        if item['periodieke_bijdrage_text'] and re.findall(r'\d+', item['periodieke_bijdrage_text']):
            periodieke_bijdrage = re.findall(r'\d+ per maand', item['periodieke_bijdrage_text'].replace('.',''))[0]  
            item['periodieke_bijdrage'] = int(periodieke_bijdrage.replace('per maand', '').strip())
        else: 
            item['periodieke_bijdrage'] = None

        # kamers
        rooms = re.findall('\d+ kamer',item['kamers_text'])
        item['kamers'] = int(rooms[0].replace(' kamer','')) if rooms else None
        bedrooms = re.findall('\d+ slaapkamer',item['kamers_text'])
        item['slaapkamers'] = int(bedrooms[0].replace(' slaapkamer','')) if bedrooms else None

        #badkamers en toiletten
        badkamers = re.findall('\d+ badkamer', item['badkamers_text'])
        item['badkamers'] = int(badkamers[0].replace(' badkamer','')) if badkamers else None
        toiletten = re.findall('\d+ apart', item['badkamers_text'])
        item['toiletten'] = int(toiletten[0].replace(' apart','')) if toiletten else None

        #balkon/dakterras    
        item['frans_balkon'] = "frans balkon" in item['balkon_of_dakterras']
        item['dakterras'] = "dakterras" in item['balkon_of_dakterras']
        item['balkon'] = len(re.findall('balkon', item['balkon_of_dakterras'])) > len(re.findall('frans balkon', item['balkon_of_dakterras']))
        
        # garage capaciteit
        garage_capaciteit = re.findall('\d+ auto', item['garage_capaciteit_text'])
        item['garage_capaciteit'] = int(garage_capaciteit[0].replace(' auto','')) if garage_capaciteit else None

        # 1.088 m² (8m diep en 11m breed)
        achtertuin = item['achtertuin_text'].replace('.','')
        achtertuin_diepte = re.findall('\d+m diep', achtertuin)
        item['achtertuin_diepte'] = int(achtertuin_diepte[0].replace('m diep', '')) if achtertuin_diepte else None
        achtertuin_breedte = re.findall('\d+m breed', achtertuin)
        item['achtertuin_breedte'] = int(achtertuin_breedte[0].replace('m breed', '')) if achtertuin_breedte else None
        achtertuin_oppervlakte = re.findall('\d+ m', achtertuin)
        item['achtertuin_oppervlakte'] = int(achtertuin_oppervlakte[0].replace(' m', '')) if achtertuin_oppervlakte else None

        voortuin = item['voortuin_text'].replace('.','')
        voortuin_diepte = re.findall('\d+m diep', voortuin)
        item['voortuin_diepte'] = int(voortuin_diepte[0].replace('m diep', '')) if voortuin_diepte else None
        voortuin_breedte = re.findall('\d+m breed', voortuin)
        item['voortuin_breedte'] = int(voortuin_breedte[0].replace('m breed', '')) if voortuin_breedte else None
        voortuin_oppervlakte = re.findall('\d+ m', voortuin)
        item['voortuin_oppervlakte'] = int(voortuin_oppervlakte[0].replace(' m', '')) if voortuin_oppervlakte else None

        item['achtertuin'] = len(item['achtertuin_text'].strip()) > 0 or 'achtertuin' in item['tuin_text']
        item['voortuin'] = len(item['voortuin_text'].strip()) > 0 or 'voortuin' in item['tuin_text']
        item['zijtuin'] = 'zijtuin' in item['tuin_text']
        item['tuin_rondom'] = 'rondom' in item['tuin_text']
        item['patio'] = 'patio' in item['tuin_text']
        item['zonneterras'] = 'zonneterras' in item['tuin_text']

        if "noordwest" in item['ligging_tuin_text']: item['ligging_tuin'] = 'NW' 
        elif "noordoost" in item['ligging_tuin_text']: item['ligging_tuin'] = 'NO' 
        elif "zuidwest" in item['ligging_tuin_text']: item['ligging_tuin'] = 'ZW' 
        elif "zuidoost" in item['ligging_tuin_text']: item['ligging_tuin'] = 'ZO' 
        elif "noorden" in item['ligging_tuin_text']: item['ligging_tuin'] = 'N' 
        elif "westen" in item['ligging_tuin_text']: item['ligging_tuin'] = 'W' 
        elif "oosten" in item['ligging_tuin_text']: item['ligging_tuin'] = 'O' 
        elif "zuiden" in item['ligging_tuin_text']: item['ligging_tuin'] = 'Z' 
        else: item['ligging_tuin'] = ''

        item['achterom'] =  "achterom" in item['ligging_tuin_text']
        

        
        woonlagen = re.findall('\d+ woonla',item['woonlagen_text'])
        item['woonlagen'] = int(woonlagen[0].replace(' woonla','')) if woonlagen else None
        item['kelder'] = "kelder" in item['woonlagen_text']
        item['vliering'] = "vliering" in item['woonlagen_text']
        item['zolder'] = "zolder" in item['woonlagen_text']
    
        if "begane grond" in item['gelegen_op_text']: 
            item['verdieping'] = 0
        else:
            item['verdieping'] = int(re.findall(r'\d+', item['gelegen_op_text'])[0]) if item['gelegen_op_text'] else None
            
        
        eigendoms_info = (item["eigendomssituatie_text"] + item["lasten_text"]).replace('.', '')
        if "erfpacht" in eigendoms_info:
            item['eigendomssituatie'] = 'erfpacht'
        elif "volle eigendom" in item["eigendomssituatie_text"]:
            item['eigendomssituatie'] = 'volle eigendom'
        else:
            item['eigendomssituatie'] = ''
        
        eind_datum_erfpacht = re.findall(r'\d{2}-\d{2}-\d{4}', item["eigendomssituatie_text"] + item["lasten_text"])
        item['eind_datum_erfpacht'] = int(eind_datum_erfpacht[0][-4:]) if eind_datum_erfpacht else None
        
        kosten_erfpacht = re.findall(r'(\d+,\d+|\d+) per jaar', eigendoms_info)
        afgekocht = "afgekocht" in (eigendoms_info)
        if afgekocht or item['eigendomssituatie'] == 'volle eigendom': 
            item["kosten_erfpacht"] = 0.0 
        elif kosten_erfpacht: 
            item["kosten_erfpacht"] = float(kosten_erfpacht[0].replace(' per jaar', '').replace(',', '.'))  
        elif 'einddatum' in eigendoms_info:
            item["kosten_erfpacht"] = 0.0 
        else: 
            item["kosten_erfpacht"] = None
        return item

class StoragePipeline(object):
    def process_item(self, item, spider):
        
        house = {
            'PartitionKey': item['gemeente'],
            'RowKey': item['postcode'] + " " + item['huisnummer'],
            
            'url' :  dict(item).get('url', ''),
            'title' :  dict(item).get('title', ''),
            'vraagprijs_text' : dict(item).get('vraagprijs_text', ''),
            'periodieke_bijdrage_text' : dict(item).get('periodieke_bijdrage_text', ''),
            'bouwjaar_text' : dict(item).get('bouwjaar_text', ''),
            'woonoppervlakte_text' :  dict(item).get('woonoppervlakte_text', ''),
            'kamers_text':  dict(item).get('kamers_text', ''),
            'soort_woning' : dict(item).get('soort_woning', ''),
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
            'ligging_tuin_text' : dict(item).get('ligging_tuin_text', ''),
            'balkon_of_dakterras' : dict(item).get('balkon_of_dakterras', ''),
            'schuur_of_berging' : dict(item).get('schuur_of_berging', ''),
            'garage' : dict(item).get('garage', ''),
            'garage_capaciteit_text' : dict(item).get('garage_capaciteit_text', ''),
            'parkeergelegenheid' : dict(item).get('parkeergelegenheid', ''),
            
            
            # derived values

            'woningtype' : dict(item).get('woningtype', ''),
            'gemeente' : dict(item).get('gemeente', ''),
            'postcode' : dict(item).get('postcode', ''),
            'postcode_regio' : dict(item).get('postcode_regio', ''),
            'postcode_wijk' : dict(item).get('postcode_wijk', ''),
            'straat' : dict(item).get('straat', ''),
            'huisnummer' : dict(item).get('huisnummer', ''),

            'vraagprijs' : dict(item).get('vraagprijs', ''),
            'kosten_koper' : dict(item).get('kosten_koper', ''),

            'bouwjaar' : dict(item).get('bouwjaar', ''),
            
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
            'ligging_tuin' : dict(item).get('ligging_tuin', ''),
            'achterom' : dict(item).get('achterom', ''),
            'achtertuin' : dict(item).get('achtertuin', ''),
            'voortuin' : dict(item).get('voortuin', ''),
            'zijtuin' : dict(item).get('zijtuin', ''),
            'tuin_rondom' : dict(item).get('tuin_rondom', ''),
            'patio' : dict(item).get('patio', ''),
            'zonneterras' : dict(item).get('zonneterras', ''),
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

        }
        # table_service.insert_or_replace_entity('HousesForSale', house)        
        table_service.insert_or_replace_entity('HouseData', house)        
        return item


