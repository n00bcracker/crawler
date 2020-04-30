
import re
from extractors import OrgExtractor, AddressExtractor
from natasha.grammars.address import Index, Country, Region, Settlement, Street, Building, Room

class Entities:
    def __init__(self):
        self._persons = set()
        self._companies = set()
        self._cities = set()

    @property
    def persons(self):
        return self._persons

    @persons.setter
    def persons(self, value):
        self._persons = value

    @property
    def companies(self):
        return self._companies

    @companies.setter
    def companies(self, value):
        self._companies = value

    @property
    def cities(self):
        return self._cities

    @cities.setter
    def cities(self, value):
        self._cities = value


class EntityExtractor:
    def __init__(self):
        self.org_extractor = OrgExtractor()
        self.addr_extractor = AddressExtractor()
        # self.bad_symbols_comp_pattern = re.compile(r'[~:;^%@#$*\[\]<>(){}\\\/!?&\n\r\x00\xa0,\|]')
        # self.concat_symbols_pattern = re.compile(r'[\u2010\u2011\u2012\u2013\u2014\u2015]')

        self.abbr_types = {
            'открытое акционерное общество' : 'ОАО',
            'общество с ограниченной ответственностью' : 'ООО',
            'закрытое акционерное общество': 'ЗАО',
            'публичное акционерное общество' : 'ПАО',
            'индивидуальный предприниматель' : 'ИП',
            'акционерное общество': 'АО',
        }

    # def __read_companies_types(self, filename):
    #     companies_types_file = open(filename, 'r')
    #     self.companies_types = list()
    #     self.comp_types_vocab = set()
    #     for line in companies_types_file:
    #         record = line.split(';')
    #         if len(record) == 2:
    #             type = dict()
    #             type['type_name'] = record[0].strip()
    #             types_list = type['type_name'].split()
    #             self.comp_types_vocab.update(types_list)
    #             type['full_name'] = [self.morph.parse(word)[0].normal_form for word in types_list]
    #             self.comp_types_vocab.update(type['full_name'] )
    #             short_type = record[1].strip()
    #             if len(short_type) > 0:
    #                 type['short_name'] = short_type
    #                 self.comp_types_vocab.add(short_type)
    #
    #             self.companies_types.append(type)
    #     companies_types_file.close()
    #
    # def __read_cities_names(self, filename):
    #     cities_file = open(filename, 'r')
    #     self.cities = list()
    #     self.cities_vocab = set()
    #     for line in cities_file:
    #         city = dict()
    #         line = line.strip()
    #         city['city_name'] = line
    #         words_list = line.split()
    #         words_list = [word.lower() for word in words_list]
    #         self.cities_vocab.update(words_list)
    #         city['name'] = words_list
    #         self.cities.append(city)
    #     cities_file.close()

    def entities_extraction(self, text):
        info = Entities()


        if len(text) > 20000:
            text = text[:10000] + text[-10000:]

        found_companies = dict()
        matches = self.org_extractor(text)
        for match in matches:
            comp = [x for x in match.fact]
            descr, type, gent, name = comp
            if type in self.abbr_types:
                type = self.abbr_types[type]
            comp = [descr, type, gent, name]
            comp = [x for x in comp if x is not None]
            comp_name = ' '.join(comp)
            comp_name = re.sub(r'[«„ʼ"\'”]', '«', comp_name, 1)
            comp_name = comp_name[::-1]
            comp_name = re.sub(r'[»“ʻ"\'”]', '»', comp_name, 1)
            comp_name = comp_name[::-1]
            if not re.search(r'банк\b', comp_name.lower()):
                if comp_name in found_companies:
                    found_companies[comp_name] += 1
                else:
                    found_companies[comp_name] = 1

        found_companies = [(name, cnt) for name, cnt in found_companies.items()]
        found_companies = sorted(found_companies, key=lambda x : x[1], reverse=True)
        found_companies = [name for name, cnt in found_companies]

        # CHANGE PARAMETERS
        found_cities = dict()
        matches = self.addr_extractor(text)
        for match in matches:
            settl = [x for x in match.fact.parts if isinstance(x, Settlement)]
            if len(settl) > 0:
                type = settl[0].type if settl[0].type is not None else ''
                name = settl[0].name if settl[0].name is not None else ''
                type = type.lower()
                key_name = name.lower()
                if key_name in found_cities:
                    found_cities[key_name]['cnt'] += 1
                    if len(type) > len(found_cities[key_name]['type']):
                        found_cities[key_name]['type'] = type
                else:
                    found_cities[key_name] = {'type' : type, 'name' : name, 'cnt' : 1,}

        found_cities = [(city['type'] + ' ' + city['name'], city['cnt']) for key_name, city in found_cities.items()]
        found_cities = sorted(found_cities, key=lambda x: x[1], reverse=True)
        found_cities = [name.strip() for name, cnt in found_cities]

        info.companies = found_companies[:10]
        info.cities = found_cities[:10]

        return info