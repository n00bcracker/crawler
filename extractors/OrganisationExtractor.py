
from yargy.pipelines import morph_pipeline, caseless_pipeline, pipeline
from yargy.tokenizer import QUOTES, LEFT_QUOTES, RIGHT_QUOTES, GENERAL_QUOTES, Tokenizer, MorphTokenizer
from yargy.interpretation import attribute, fact
from yargy.rule.transformators import RuleTransformator

from .NamesExtractor import SIMPLE_NAME
from .SettlementExtractor import RESPUBLIKA, KRAI, OBLAST, AUTO_OKRUG, RAION, GOROD, SELO, POSELOK, DEREVNYA
from .PersonExtractor import POSITION_NAME
from .CappedParser import CappedExtractor

from yargy import (
    rule,
    not_,
    and_,
    or_,
)

from yargy.predicates import (
    eq,
    in_,
    true,
    gram,
    type,
    caseless,
    normalized,
    is_capitalized,
    is_single
)

from yargy.relations import (
    gnc_relation,
    case_relation,
)

class StripInterpretationTransformator(RuleTransformator):
    def visit_InterpretationRule(self, item):
        return self.visit(item.rule)

NAME = SIMPLE_NAME.transform(StripInterpretationTransformator)
PERSON = POSITION_NAME.transform(StripInterpretationTransformator)

BAD_SYMBOLS = ';'

ABBR_TYPE = pipeline([
    'АО',
    'ОАО',
    'ООО',
    'ЗАО',
    'ПАО',
    'ИП',
    'БФ',
    'ГСК',
    'ГБУ',
    'ГКУ',
    'ГУП',
    'Д/С',
    'ДСУ',
    'ДОУч',
    'ЖСК',
    'КБ',
    'КФХ',
    'МУУП',
    'МУУЧ',
    'МКУ',
    'МБУ',
    'НПО',
    'НПП',
    'НТЦ',
    'ОДО',
    'ПИФ',
    'ПРОФКОМ',
    'РедСМИ',
    'РСУ',
    'РЭУ',
    'СНТ',
    'СМУ',
    'ТСЖ',
    'ТД',
    'ФГУП',
    'ФКП',
    'ФБУ',
    'ФГУ',
    'ФГБУ',
    'ФКУ',
    'ЧОП',
    'ЧИФ',
    'Я/С',
])

TYPE = morph_pipeline([
    'б-ца',
    'з-д',
    'ин-т',
    'п-ка',

    'аптека',
    'магазин',
    'больница',
    'детский сад',
    'монастырь',
    'поликлиника',
    'церковь',
    'лицей',
    'ясли-сад',
    'нии',
    'академия',
    'обсерватория',
    'университет',
    'институт',
    'политех',
    'колледж',
    'техникум',
    'училище',
    'школа',
    'музей',
    'библиотека',

    'кооператив',
    'предприятие',
    'артель',
    'ассоциация',
    'учреждение',
    'колхоз',
    'фирма',
    'фонд',
    'банк',
    'центр',
    'бюро',
    'товарищество',
    'отделение',
    'организация',
    'общество',
    'партия',
    'представительство',
    'приход',
    'община',
    'комитет',
    'совхоз',
    'филиал',
    'агентство',
    'компания',
    'издательство',
    'газета',
    'концерн',
    'завод',
    'корпорация',
    'группа компаний',
    'санаторий',
    'подразделение',

    'кафе',
    'ресторан',
    'закусочная',

    'авиакомпания',
    'госкомпания',
    'инвесткомпания',
    'медиакомпания',
    'оффшор-компания',
    'радиокомпания',
    'телекомпания',
    'телерадиокомпания',
    'траст-компания',
    'фактор-компания',
    'холдинг-компания',
    'энергокомпания',
    'компания-производитель',
    'компания-изготовитель',
    'компания-заказчик',
    'компания-исполнитель',
    'компания-посредник',
    'группа управляющих компаний',
    'агрофирма',
    'турфирма',
    'юрфирма',
    'фирма-производитель',
    'фирма-изготовитель',
    'фирма-заказчик',
    'фирма-исполнитель',
    'фирма-посредник',
    'авиапредприятие',
    'агропредприятие',
    'госпредприятие',
    'нацпредприятие',
    'промпредприятие',
    'энергопредприятие',
    'авиакорпорация',
    'госкорпорация',
    'профорганизация',
    'стартап',
    'нотариальная контора',
    'букмекерская контора',
    'авиазавод',
    'автозавод',
    'винзавод',
    'подстанция',
    'гидроэлектростанция',
    'общество',
    'акционерное общество',
    'открытое акционерное общество',
    'общество с ограниченной ответственностью',
    'закрытое акционерное общество',
    'публичное акционерное общество',
    'индивидуальный предприниматель',
    'некоммерческая организация',
    'адвокатская палата',
    'благотворительный фонд',
    'внебюджетный фонд',
    'государственное бюджетное учреждение',
    'государственное казенное учреждение',
    'государственное предприятие',
    'государственное унитарное предприятие',
    'государственное учреждение',
    'дачное товарищество',
    'дорожное строительное управление',
    'образовательное учреждение',
    'жилищно-строительный кооператив',
    'конструкторское бюро',
    'кредитный союз',
    'крестьянское фермерское хозяйство',
    'крестьянское хозяйство',
    'личное подсобное хозяйство',
    'малое предприятие',
    'межгосударственная финансово-промышленная группа',
    'муниципальное предприятие',
    'муниципальное казенное предприятие',
    'муниципальное унитарное предприятие',
    'муниципальное учреждение',
    'муниципальное казенное учреждение',
    'муниципальное бюджетное учреждение',
    'научно-производственная фирма',
    'научно-производственное объединение',
    'научно-производственное предприятие',
    'некоммерческое партнерство',
    'нотариальная контора',
    'нотариальная палата',
    'обособленное подразделение',
    'обособленное структурное подразделение',
    'объединение крестьянских фермерских хозяйств',
    'общероссийский профсоюз',
    'общественная организация',
    'общественное движение',
    'общественное объединение',
    'общественное учреждение',
    'общественный фонд',
    'общество с дополнительной ответственностью',
    'объединение предприятий',
    'объединение фермерских хозяйств',
    'орган общественной самодеятельности',
    'паевой инвестиционный фонд',
    'потребительский союз',
    'потребительское общество',
    'производственное объединение',
    'редакция средств массовой информации',
    'религиозная организация',
    'религиозное общество',
    'ремонтно-строительное управление',
    'ремонтно-эксплуатационное управление',
    'садоводческое товарищество',
    'садоводческое некоммерческое товарищество',
    'союз крестьянских фермерских хозяйств',
    'союз потребительских обществ',
    'строительно-монтажное управление',
    'структурное подразделение',
    'территориальная организация профсоюза',
    'территориальное общественное самоуправление',
    'товарищество на вере',
    'товарищество собственников жилья',
    'торговый дом',
    'управление производственно-технической комплектации',
    'управление делами',
    'федеральное государственное унитарное предприятие',
    'федеральное казенное предприятие',
    'федеральное бюджетное учреждение',
    'федеральное государственное учреждение',
    'федеральное государственное бюджетное учреждение',
    'федеральное казенное учреждение',
    'финансово-промышленная группа',
    'хозяйственное управление',
    'частное охранное предприятие',
    'чековый инвестиционный фонд',
])

Organisation = fact(
    'Organisation',
    ['descr', 'type', 'gent', 'name']
)

gnc = gnc_relation()

ADJF_PREFIX = rule(
    or_(
        and_(
            is_single(),
            gram('ADJF'),
            not_(gram('Apro')),
        ).match(gnc),  # международное
        rule(  # историко-просветительское
            and_(
                not_(type('LATIN')),
                not_(gram('NPRO')),
            ),
            eq('-'),
            and_(
                is_single(),
                gram('ADJF'),
                not_(gram('Apro')),
            ).match(gnc),
        ),
    ),
    or_(caseless('и'), eq(',')).optional(),
).repeatable(max=6).interpretation(Organisation.descr.inflected({'nomn'}))

ADJF_PREFIX_CAP = rule(
    or_(
        and_(
            is_capitalized(),
            is_single(),
            gram('ADJF'),
            not_(gram('Apro')),
        ).match(gnc),  # международное
        rule(  # историко-просветительское
            and_(
                not_(type('LATIN')),
                is_capitalized(),
                not_(gram('NPRO')),
            ),
            eq('-'),
            and_(
                is_single(),
                gram('ADJF'),
                not_(gram('Apro')),
            ).match(gnc),
        ),
    ),
    rule(
        or_(caseless('и'), eq(',')).optional(),
        or_(
            and_(
                is_single(),
                gram('ADJF'),
                not_(gram('Apro')),
            ).match(gnc),  # международное
            rule(  # историко-просветительское
                and_(
                    not_(type('LATIN')),
                    is_capitalized(),
                    not_(gram('NPRO')),
                ),
                eq('-'),
                and_(
                    is_single(),
                    gram('ADJF'),
                    not_(gram('Apro')),
                ).match(gnc),
            ),
        ),
    ).optional().repeatable(max=5)
).interpretation(Organisation.descr.inflected({'nomn'}).custom(lambda x: x.capitalize()))

ORGN_TYPE = rule(
    or_(
        ABBR_TYPE.interpretation(Organisation.type),
        TYPE.match(gnc).interpretation(Organisation.type.normalized()),
    )
)

case = case_relation()

GENT_GROUP = rule(  # родительный падеж
    and_(
        gram('gent'),
        not_(gram('Abbr')),
        not_(gram('PREP')),
    ).match(case),
).repeatable(max=12).optional()

QUOTED_ORGN_NAME = or_(
    rule(
        in_(LEFT_QUOTES),
        and_(
            not_(in_(RIGHT_QUOTES)),
            not_(in_(BAD_SYMBOLS)),
        ).repeatable(max=20),
        in_(RIGHT_QUOTES),
    ),
    rule(
        in_(GENERAL_QUOTES),
        not_(in_(GENERAL_QUOTES)).repeatable(max=20),
        in_(GENERAL_QUOTES),
    ),
)

CAPITALIZED_ORGN_NAME = rule(
    and_(
        is_capitalized(),
        not_(gram('PREP')),
        not_(gram('CONJ')),
    ).repeatable(max=6)
)

ORGN_NAME = rule(
    or_(
        QUOTED_ORGN_NAME,
        CAPITALIZED_ORGN_NAME,
    )
)

NAMED = rule(
    or_(
        rule(normalized('имя')),
        rule(caseless('им'), eq('.').optional()),
    ).optional(),
    or_(
        NAME,
        PERSON,
    ),
)

ORGN_AREA = rule(
    or_(
        RESPUBLIKA,
        KRAI,
        OBLAST,
        AUTO_OKRUG,
        RAION,
        GOROD,
        SELO,
        POSELOK,
        DEREVNYA,
    )
)

NUMERED = rule(
    eq('№'),
    type('INT'),
)

ORGN_ID = rule(
    or_(
        rule(
            NUMERED,
            or_(
                NAMED,
                ORGN_AREA,
                ORGN_NAME,
                GENT_GROUP,
            ).optional(),
        ),
        rule(
            NAMED,
            ORGN_NAME.optional(),
        ),
        rule(
            ORGN_AREA,
            ORGN_NAME.optional(),
        ),
        ORGN_NAME,
    ),
).interpretation(Organisation.name)

ORGANISATION = or_(
    rule(
        ADJF_PREFIX.optional(),
        ORGN_TYPE,
        GENT_GROUP.interpretation(Organisation.gent.custom(lambda x: x.lower())),
        ORGN_ID,
    ),
    rule(
        ADJF_PREFIX_CAP,
        ORGN_TYPE,
        GENT_GROUP.interpretation(Organisation.gent.custom(lambda x: x.lower())),
    ),

).interpretation(Organisation)


class OrgExtractor(CappedExtractor):
    def __init__(self):
        super(OrgExtractor, self).__init__(ORGANISATION)

#TODO Сделать подгрузку словарей из файлов

