# coding: utf-8

from yargy import (
    rule,
    or_
)
from yargy.interpretation import fact
from yargy.predicates import gram
from yargy.pipelines import morph_pipeline

from .NamesExtractor import (
    NAME,
    SIMPLE_NAME
)


Person = fact(
    'Person',
    ['position', 'name']
)


POSITION = morph_pipeline([
    'святой',
    'патриарх',
    'митрополит',

    'царь',
    'король',
    'царица',
    'император',
    'императрица',
    'принц',
    'принцесса',
    'князь',
    'граф',
    'графиня',
    'барон',
    'баронесса',
    'княгиня',

    'президент',
    'премьер-министр',
    'экс-премьер',
    'пресс-секретарь',
    'министр',
    'замминистр',
    'заместитель',
    'глава',
    'канцлер',
    'помощник',
    'посол',
    'губернатор',
    'председатель',
    'спикер',
    'диктатор',
    'лидер',
    'генсек',
    'премьер',
    'депутат',
    'вице-премьер',
    'сенатор',
    'полпред',
    'госсекретарь',
    'вице-президент',
    'сопредседатель',
    'зам',
    'мэр',
    'вице-спикер',
    'замруководителя',
    'зампред',
    'муфтий',
    'спецпредставитель',
    'руководитель',
    'статс-секретарь',
    'зампредседатель',
    'представитель',
    'ставленник',
    'мадеро',
    'вице-губернатор',
    'зампредсовмин',
    'наркоминдела',
    'генпрокурор',
    'комиссар',
    'рейхсканцлер',
    'советник',
    'замглавы',
    'секретарь',
    'парламентарий',
    'замгендиректор',
    'вице-председатель',
    'постпред',
    'госкомтруд',
    'предсовмин',
    'преемник',
    'делегат',
    'шеф',
    'консул',
    'замминистра',
    'главкомпис',
    'чиновник',
    'врио',
    'управделами',
    'нарком',
    'донпродкомиссар',
    'председ',
    'гендиректор',
    'генерал-губернатор',
    'обревком',
    'правитель',
    'замсекретарь',
    'главнокомандующий',
    'вице-мэр',
    'наместник',
    'спичрайтер',
    'вице-консул',
    'мвэс',
    'облревком',
    'главковерх',
    'пресс-атташе',
    'торгпред',
    'член',
    'назначенец',
    'эмиссар',
    'обрядоначальник',
    'главком',
    'единоросс',
    'политик',
    'генерал',
    'замгенпрокурор',
    'дипломат',
    'главноуполномоченный',
    'генерал-фельдцейхмейстер',
    'комендант',
    'казначей',
    'уполномоченный',
    'обер-прокурор',
    'наркомзем',
    'соправитель',

    'основатель',
    'сооснователь',
    'управляющий директор',
    'управляющий партнер',
    'партнер',
    'руководитель',
    'аналитик',
    'зампред',
    'миллиардер',
    'миллионер',

    'автор',
    'актер',
    'актриса',
    'певец',
    'певица',
    'исполнитель',
    'солист',
    'режиссер',
    'сценарист',
    'писатель',
    'музыкант',
    'композитор',
    'корреспондент',
    'журналист',
    'редактор',
    'дирижер',
    'кинорежиссер',
    'звукорежиссер',
    'детектив',
    'пианист',
    'драматург',
    'артист',
    'балетмейстер',
    'скрипач',
    'хореограф',
    'танцовщик',
    'документалист',
    'поэт',
    'литератор',
    'киноактер',
    'вокалист',
    'бард',
    'комик',
    'продюсер',
    'кинодраматург',
    'киноактриса',
    'балерина',
    'пианистка',
    'критик',
    'танцор',
    'концертмейстер',
    'симфонист',
    'сатирик',
    'аранжировщик',
    'саксофонист',
    'юморист',
    'шансонье',
    'гастролер',
    'виолончелист',
    'постановщик',
    'кинематографист',
    'сценограф',
    'джазмен',
    'музыковед',
    'киноартист',
    'педагог',
    'хормейстер',
    'беллетрист',
    'примадонна',
    'инструменталист',
    'альтист',
    'шоумен',
    'виртуоз',
    'пародист',
    'декоратор',
    'модельер',
    'очеркист',
    'оперетта',
    'контрабасист',
    'карикатурист',
    'дуэт',
    'монтажер',
    'живописец',
    'скульптор',
    'режиссура',
    'архитектор',
    'антрепренер',
    'импрессарио',
    'прозаик',
    'труппа',
    'трагик',
    'клоун',
    'солистка',
    'либреттист',
    'литературовед',
    'портретист',
    'гример',
    'художник',
    'импровизатор',
    'декламаторша',
    'телеведущий',
    'импресарио',
    'мастер',
    'аккомпаниатор',
    'шахматист',
    'иллюзионист',
    'эстрадник',
    'эстрада',
    'спортсмен',
    'дизайнер',
    'кинокритик',
    'публицист',
    'чтец',
    'конферансье',
    'студиец',
    'коверный',
    'куплетист',
    'знаменитость',
    'ученый',
    'балет',
    'искусствовед',
    'гитарист',

    'доктор',

    'академик',

    'судья',
    'юрист',
    'представитель',
    'директор',
    'прокурор',

    'отец',
    'мать',
    'мама',
    'папа',
    'брат',
    'сестра',
    'тёща',
    'тесть',
    'дедушка',
    'бабушка',
    'жена',
    'муж',
    'дочь',
    'сын',

    'мистер',
    'миссис',
    'господин',
    'госпожа',
    'пан',
    'пани',
    'сэр',
    'мисс',

    'боксер',
    'боец',
    'атлет',
    'футболист',
    'баскетболист',

    'агроном',

    'президент',
    'сопрезидент',
    'вице-президент',
    'экс-президент',
    'председатель',
    'руководитель',
    'директор',
    'глава',
])

GENT = gram('gent')

WHERE = or_(
    rule(GENT),
    rule(GENT, GENT),
    rule(GENT, GENT, GENT),
    rule(GENT, GENT, GENT, GENT),
    rule(GENT, GENT, GENT, GENT, GENT),
)

POSITION = or_(
    POSITION,
    rule(POSITION, WHERE)
).interpretation(
    Person.position
)

NAME = NAME.interpretation(
    Person.name
)

SIMPLE_NAME = SIMPLE_NAME.interpretation(
    Person.name
)

POSITION_NAME = rule(
    POSITION,
    SIMPLE_NAME
)