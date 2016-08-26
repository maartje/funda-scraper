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
        item['straat'] = re.findall(r'te koop: ([a-zA-Z\. -]*) ', item['title'])[0]
        item['huisnummer'] = re.findall(r'\d+', item['title'])[0]
        
        # vraagprijs
        item['vraagprijs'] = re.findall(r' \d+.\d+', item['vraagprijs_text'])[0].strip().replace('.','')
        item['kosten_koper'] = not(re.findall(r' v\.o\.n', item['vraagprijs_text']))

        # bouwjaar
        item['bouwjaar'] = re.findall(r'\d+', item['bouwjaar_text'])[0] if item['bouwjaar_text'] else ''

        # woonoppervlakte
        item['woonoppervlakte'] = re.findall(r'\d+', item['woonoppervlakte_text'])[0] if item['woonoppervlakte_text'] else ''

        # perceel_oppervlakte
        item['perceel_oppervlakte'] = re.findall(r'\d+', item['perceel_oppervlakte_text'])[0] if item['perceel_oppervlakte_text'] else ''
        
        # inpandige ruimte
        item['inpandige_ruimte'] = re.findall(r'\d+', item['inpandige_ruimte_text'])[0] if item['inpandige_ruimte_text'] else ''

        # buitenruimte
        item['buitenruimte'] = re.findall(r'\d+', item['buitenruimte_text'])[0] if item['buitenruimte_text'] else ''

        periodieke_bijdrage = re.findall(r'\d+ per maand', item['periodieke_bijdrage_text'])[0] if item['periodieke_bijdrage_text'] and re.findall(r'\d+', item['periodieke_bijdrage_text']) else ''
        item['periodieke_bijdrage'] = periodieke_bijdrage.replace('per maand', '').strip()

        # kamers
        rooms = re.findall('\d+ kamer',item['kamers_text'])
        item['kamers'] = rooms[0].replace(' kamer','') if rooms else ''
        bedrooms = re.findall('\d+ slaapkamer',item['kamers_text'])
        item['slaapkamers'] = bedrooms[0].replace(' slaapkamer','') if bedrooms else ''

        #badkamers en toiletten
        badkamers = re.findall('\d+ badkamer', item['badkamers_text'])
        item['badkamers'] = badkamers[0].replace(' badkamer','') if badkamers else ''
        toiletten = re.findall('\d+ apart', item['badkamers_text'])
        item['toiletten'] = badkamers[0].replace(' apart','') if badkamers else ''

        #balkon/dakterras    
        frans_balkon = len(re.findall('(f|F)rans balkon', item['balkon_of_dakterras']))
        dakterras = len(re.findall('(d|D)akterras', item['balkon_of_dakterras']))
        balkon = len(re.findall('(b|B)alkon', item['balkon_of_dakterras']))
        item['frans_balkon'] = frans_balkon > 0
        item['dakterras'] = dakterras > 0
        item['balkon'] = balkon > frans_balkon
        
        # garage capaciteit
        garage_capaciteit = re.findall('\d+ auto', item['garage_capaciteit_text'])
        item['garage_capaciteit'] = garage_capaciteit[0].replace(' auto','') if garage_capaciteit else ''

        # 1.088 m² (8m diep en 11m breed)
        achtertuin = item['achtertuin'].replace('.','')
        achtertuin_diepte = re.findall('\d+m diep', achtertuin)
        item['achtertuin_diepte'] = achtertuin_diepte[0].replace('m diep', '') if achtertuin_diepte else ''
        achtertuin_breedte = re.findall('\d+m breed', achtertuin)
        item['achtertuin_breedte'] = achtertuin_breedte[0].replace('m breed', '') if achtertuin_breedte else ''
        achtertuin_oppervlakte = re.findall('\d+ m', achtertuin)
        item['achtertuin_oppervlakte'] = achtertuin_oppervlakte[0].replace(' m', '') if achtertuin_oppervlakte else ''

        voortuin = item['voortuin'].replace('.','')
        voortuin_diepte = re.findall('\d+m diep', voortuin)
        item['voortuin_diepte'] = voortuin_diepte[0].replace('m diep', '') if voortuin_diepte else ''
        voortuin_breedte = re.findall('\d+m breed', voortuin)
        item['voortuin_breedte'] = voortuin_breedte[0].replace('m breed', '') if voortuin_breedte else ''
        voortuin_oppervlakte = re.findall('\d+ m', voortuin)
        item['voortuin_oppervlakte'] = voortuin_oppervlakte[0].replace(' m', '') if voortuin_oppervlakte else ''
        
        woonlagen = re.findall('\d+ woonla',item['woonlagen_text'])
        item['woonlagen'] = woonlagen[0].replace(' woonla','') if woonlagen else ''
        item['kelder'] = "kelder" in item['woonlagen_text']
        item['vliering'] = "vliering" in item['woonlagen_text']
        item['zolder'] = "zolder" in item['woonlagen_text']
    
            
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
            'buitenruimte_text' : dict(item).get('buiten_ruimte_text', ''),
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
            'tuin' : dict(item).get('tuin', ''),
            'achtertuin' : dict(item).get('achtertuin', ''),
            'voortuin' : dict(item).get('voortuin', ''),
            'ligging_tuin' : dict(item).get('ligging_tuin', ''),
            'balkon_of_dakterras' : dict(item).get('balkon_of_dakterras', ''),
            'schuur_of_berging' : dict(item).get('schuur_of_berging', ''),
            'garage' : dict(item).get('garage', ''),
            'garage_capaciteit' : dict(item).get('garage_capaciteit', ''),
            'parkeergelegenheid' : dict(item).get('parkeergelegenheid', ''),
            
            
            # derived values

            'woningtype' : dict(item).get('woningtype', ''),
            'gemeente' : dict(item).get('gemeente', ''),
            'postcode' : dict(item).get('postcode', ''),
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

            'periodieke_bijdrage' : dict(item).get('periodieke_bijdrage', ''),

            
#            'aanbod_jaar' : datetime.datetime.now().year,
#            'aanbod_maand' : datetime.datetime.now().month,
#            'sale_date' : dict(item).get('sale_date', '')
        }
        table_service.insert_or_replace_entity('HousesForSale', house)        
        return item


