""" helpers

    25/Set/2020 - José Carlos Cordeiro

    Este módulo contém um conjunto de funções de apoio para lidar com documentos Json.

    Functions:
        adjustText(txt: str, case:int = 0) -> str
        str2date(dd: str) -> date
        getDocAttributes(attribs:dict, obj: dict, prefix: str = '')
        getDataType(dt:any) -> str
        dumpBulkElastik(jList: list) -> str
"""
import copy
import re
from datetime import datetime, date
import cProfile
import pstats

# import orjson as js
# import jsondatetime as js
# import jsjson as js
import unidecode

HELPER_NOACCENTS = 0
HELPER_CAMELCASE = 1
HELPER_PASCALCASE = 2

refind = re.compile(r".*'(.*)'.*")  # RegEx para extrair o tipo do atributo


def getDateGroup(dateValue: datetime, groupType: str) -> str:
    if not dateValue:
        return "None"
    # endif --

    y = f"00{dateValue.year}"[-4:]
    m = f"00{dateValue.month}"[-2:]
    d = f"00{dateValue.day}"[-2:]
    w = f"00{date(dateValue.year, dateValue.month, dateValue.day).isocalendar()[1]}"[-2:]

    match groupType:
        case 'day':
            label = f"{y}-{m}-{d}"
        case 'week':
            label = f"{y}-{w}"
        case 'month':
            label = f"{y}-{m}"
        case _:
            label = str(y)
    # endmatch --

    return label


def parseDatetime(datetime_str) -> bool:
    if not str2datetime(datetime_str):
        return False
    # endif --

    return True


def str2datetime(datetime_str) -> datetime | None:
    if not datetime_str:
        return None
    # endif --

    if isinstance(datetime_str, datetime):
        # já é datetime
        return datetime_str
    # endif --

    if isinstance(datetime_str, date):
        # é date
        return datetime.fromordinal(datetime_str.toordinal())
    # endif --

    if isinstance(datetime_str, str):
        datetime_str = datetime_str.replace('/', '-')

        if len(datetime_str) > 19:
            # AAAA-MM-DDTHH:MM:SS
            datetime_str = datetime_str[:19]
        else:
            flag = False
            men = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            for mm in men:
                if mm in datetime_str:
                    flag = True
                    datetime_str = datetime_str.replace(mm, str(men.index(mm) + 1))
                    break
                # endif --
            # endfor --

            if not flag:
                mpt = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dec']
                for mm in mpt:
                    if mm in datetime_str:
                        flag = True
                        datetime_str = datetime_str.replace(mm, str(mpt.index(mm) + 1))
                        break
                    # endif --
                # endfor --
            # endif --

            if flag:
                try:
                    return datetime.strptime(datetime_str, "%d-%m-%Y")
                except ValueError:
                    pass

                try:
                    return datetime.strptime(datetime_str, "%d-%m-%y")
                except ValueError:
                    pass
        # endif --
    # endif --

    try:
        return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        pass

    try:
        return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M")
    except ValueError:
        pass

    try:
        return datetime.strptime(datetime_str, "%Y-%m-%d")
    except ValueError:
        pass

    try:
        return datetime.strptime(datetime_str, "%y-%m-%d")
    except ValueError:
        pass

    try:
        return datetime.strptime(datetime_str, "%d-%m-%Y %H:%M")
    except ValueError:
        pass

    try:
        return datetime.strptime(datetime_str, "%d-%m-%y %H:%M")
    except ValueError:
        pass

    return None


def datetime2str(dt: datetime) -> str | None:
    if not dt:
        return None

    return dt.strftime("%m/%d/%Y")


def str2date(date_str: str) -> date | None:
    """
    Converte string para data.

    Args:
        date_str: string com data

    Returns:
        Date:
    """
    if not date_str:
        return None
    # endif --

    if isinstance(date_str, date):
        # já é date
        return date_str
    # endif --

    tk = date_str.split('-')
    return date(int(tk[0]), int(tk[1]), int(tk[2]))


def date2str(dd: date) -> str:
    """
    Converte data para string.

    Args:
        dd: data

    Returns:
        str: string com data
    """
    return '{:%Y-%m-%d}'.format(dd)


def adjustText(txt: str, case: int = 0) -> str:
    """
    Ajusta um texto para CamelCase, PascalCase assim como remove os caracteres de pontuação, os não alfanuméricos e os '_'

    Args:
        txt (str): texto a ser ajustado.\n
        case (int, optional): ajuste a ser executado no texto:
                    0 remove caracteres de pontuação
                    1 camelCase
                    2 PascalCase

    Returns:
        str: texto ajustado.
    """
    if not txt.strip():
        return ''
    # endif --

    # tira os caracteres não alfanuméricos
    s = unidecode.unidecode(txt)
    s = ''.join((c if c.isalnum() else '_') for c in s)
    s = s.replace('__', '_')  # tira os "_" duplos

    if case != 0:
        # coloca a primeira letra de cada palavra em maiúsculo
        s = ''.join(x.capitalize() for x in s.split('_'))

        if case == 1:
            # camelCase - muda a primeira letra para minúscula
            s = s[0].lower() + s[1:]

        else:
            # PascalCase - muda a primeira letra para maiúscula
            s = s[0].upper() + s[1:]
        # endif --
    # endif --

    return s


def escapeRegEx(txt: str) -> str:
    """
    Adiciona '\' aos caracteres de RegEx.

    Args:
        txt (str): texto cujos caracters serão tratados

    Returns:
        str: texto onde os caracters de RegEx estão com escape
    """
    for c in '.^$*+?{}[]()|':
        txt = txt.replace(c, f'\\{c}')

    return txt


def getDocAttributes(attribs: dict, obj: dict, prefix: str = '', flagDeepDocs: bool = True) -> dict:
    """
    Monta um dicionário com todos os atributos de um  documento Json

    Args:
        attribs (dict): dicionário onde serão armazenados os atributos
        obj (dict): dicionário contendo o objeto/documento Json
        prefix (str, optional): prefixo a ser adicionado ao nome dos atributos. Defaults to ''.
        flagDeepDocs (bool, optional): se True coleta também os atributos de documentos aninhados.

    Returns:
        dict: dicionário onde serão armazenados os atributos (attribs)
    """

    # cataloga cada item do dicionário (atributos do objeto)
    for key in obj:
        # se houver prefixo, concatena ao nome do atributo
        at = key if not prefix else f"{prefix}.{key}"

        if obj[key] is None:
            tp = None

        elif isinstance(obj[key], dict):
            # se o atributo contem um outro dicionário aninhado, um objeto, então indica o tipo como 'Objeto'
            tp = 'Object'

        elif isinstance(obj[key], list):
            # se o abributo contem uma lista
            tp = 'Array | Empty' if len(obj[key]) == 0 else \
                ('Array | Object' if isinstance(obj[key][0], dict) else 'Array | Value')

        else:
            # senão pega o tipo nativo do valor do atributo
            tp = getDataType(obj[key])
        # endif --

        # se já tiver catalogado um atributo com este nome, concatena o novo tipo, senão apenas adiciona
        if at in attribs.keys():
            if attribs[at] != tp:
                attribs[at] = f"{attribs[at]} | {tp}"
            # endif --
        else:
            attribs[at] = tp
        # endif --

        # se for para cataloga os atributos de objetos aninhados
        if flagDeepDocs:
            if isinstance(obj[key], dict):
                # se o atributo contem um dicionário aninhado (objeto)
                getDocAttributes(attribs, obj[key], at)

            elif isinstance(obj[key], list):
                # se o atributo contem uma lista de objetos
                for item in obj[key]:
                    if isinstance(item, dict):
                        getDocAttributes(attribs, item, at)
                    # endif --
                # endfor --
            # endif --
        # endif --
    # endfor --

    return attribs


def getDataType(var: any) -> str:
    """
    Retorna um string com o tipo de dado de uma variável

    Args:
        var (any): variável cujo tipo será analisado

    Returns:
        str: string com o tipo
    """
    gp = refind.search(str(type(var)))
    tp = gp.group(1)
    return tp.split('.')[0]


def dumpBulkElastik(jlist: list, idAttrib: str, filename: str = None) -> str:
    """
    Cria um script de BULK INSERT para o Eslastic com os objectos da lista.

    Args:
        jlist (list): list de dicionários com documento Json
        idAttrib: nome do atributo que será usado como ID
        filename (str): arquivo para gravação do arquivo de BULK INSERT

    Returns:
        str: string contendo o documento
    """
    if filename:
        jFile = open(filename, "w", encoding='utf-8')
    else:
        jFile = None
    # endif --

    # itera em cada objeto da lista
    s = ''
    for jDoc in jlist:
        idx = '{ "index" : { "_id" : "' + jDoc.get(idAttrib) + '" } }\n'

        if not filename:
            s += idx
            s += f'{jDoc.getJson(flagPretty=False)}\n'
        else:
            jFile.write(idx)
            jFile.write(f'{jDoc.getJson(flagPretty=False)}\n')
        # ndeif --
    # endif --

    if not filename:
        return s
    # endif --

    jFile.close()

    return ''


def startProfiling():
    profiling = cProfile.Profile()
    profiling.enable()
    return profiling


def stopProfiling(profiling: cProfile.Profile, sortField: str = None, profFile: str = None, flagBR: bool = True):
    # sortField = ncalls, tottime, percall, cumtime, percall
    profiling.disable()

    if profFile:
        profiling.dump_stats(profFile)
        profiling2csv(profiling, csvFile=f"{ profFile.replace('.', '_') }.csv", flagBR=flagBR)

        profStats = pstats.Stats(profFile)

        if sortField:
            profStats.sort_stats(sortField)
        # endif --

        # profStats.print_stats()

    else:
        profiling.print_stats()
    # endif --


def profiling2csv(prof: cProfile.Profile, csvFile: str, flagBR: bool = True):
    import io

    out_stream = io.StringIO()
    pstats.Stats(prof, stream=out_stream).print_stats()
    result = out_stream.getvalue()
    # chop off header lines
    result = 'ncalls' + result.split('ncalls')[-1]
    result = '\n'.join([','.join(line.rstrip().split(None, 5)) for line in result.split('\n')])

    if flagBR:
        result = result.replace(',', ';')
        result = result.replace('.', ',')
    # endif --

    with open(csvFile, 'w') as f:
        # f=open(result.rsplit('.')[0]+'.csv','w')
        f.write(result)
        f.close()
    # endwith --

    return


def sortDictionary(dicToSort: dict) -> dict:
    dicClone = dict(sorted(copy.deepcopy(dicToSort).items()))
    dicToSort.clear()
    dicToSort.update(dicClone)
    return dicToSort
