""" json2table

    7/Set/2020 - José Carlos Cordeiro

    Este e módulo provê um conjunto de facilitadores para transformar uma lista de documentos Json,
    armazenadas num string ou num list[] num pandas.DataFrame.

    Functions:
        json2table(jdata: str , prefix: str = None, fields: list = None, ignore: list = None) -> pandas.DataFrame
        dict2table(records: list, prefix: str = None, fields: list = None, ignore: list = None) -> pandas.DataFrame
"""

import collections

from pandas import DataFrame

from JsonObjects import jsjson as js


def dict2table(objs: list, flagExpandValueLists: bool = False, flagExpandObjectLists: bool = False, flagRemovePrefix: bool = False) -> DataFrame:
    """Converte um dicionário derivado de um objeto Json numa tabela.

    Args:
        objs (list): lista de objetos Json a serem convertidos em tabela
        flagExpandValueLists (bool, optional): se True expande os campos com listas de valores, tratando-as como tabelas associadas num CROSS-JOIN
        flagExpandObjectLists (bool, optional): se True expande os campos com listas de objetos, tratando-as como tabelas associadas num CROSS-JOIN
        flagRemovePrefix (bool, optional): se True os prefixos serão removidos dos nomes das colunas

    Returns:
        [pandas.DataFrame]: tabela com os objetos.
    """

    tab = {}

    def _flatten(d, parent_key: str = '', sep: str = '.') -> dict:
        items = []
        for kk, vv in d.items():
            new_key = parent_key + sep + kk if parent_key else kk
            if isinstance(vv, collections.MutableMapping):
                items.extend(_flatten(vv, new_key, sep=sep).items())
            else:
                items.append((new_key, vv))
        # endfor --

        return dict(items)
    # endif --

    def _adicionarColuna(colName: str, val: any):
        # pega a quantidade de linhas da tabela
        if len(tab.keys()) == 0:
            # tabela vazia
            qdeCol = 0
        else:
            # tabela com pelo menos uma linha
            col = list(tab.keys())[0]
            qdeCol = len(tab[col])

        # se a coluna não existe cria a coluna
        if colName not in tab.keys():
            tab[colName] = []

            if qdeCol > 1:
                # insere as linhas anteriores com vazio
                for lin in range(qdeCol - 1):
                    tab[colName].append(None)

        tab[colName].append(val)
    # enddef --

    def _adicionarLinha(obj: dict):
        # achata o objeto, contatenando as chaves
        newObj = _flatten(obj)

        # para cada atributo do objeto, adiciona uma coluna
        for kk, vv in newObj.items():
            _adicionarColuna(kk, vv)
    # enddef --

    # cria a coleção de linhas
    for objItem in objs:
        _adicionarLinha(objItem)

    # se solicitado, expande as listas que existirem em qualquer das colunas
    if flagExpandValueLists or flagExpandObjectLists:
        flagContemListas = True
        linToDelete = []
        while flagContemListas:
            flagContemListas = False
            colunas = list(tab.keys())
            # para cada coluna
            for coluna in colunas:
                # trata uma coluna
                linhas = tab[coluna]
                flagObj = False

                # esta coluna contem valores do tipo lista (pega o tipo do valor na primeira linha da tabela - linha 0)
                numLinha = -1
                for valLin in linhas:
                    numLinha += 1

                    # se é uma lista de objetos
                    if isinstance(valLin, list):
                        flagContemListas = True
                        lstItem = valLin.copy()

                        # se o primeiro item da lista é um objeto
                        if flagExpandObjectLists and isinstance(lstItem[0], dict):
                            # levanta a flag para remover a coluna
                            flagObj = True
                            # removeAttrib a lista de objetos
                            tab[coluna][numLinha] = None
                            linToDelete.append(numLinha)

                        # senão, se é uma lista de valores atômicos
                        elif flagExpandValueLists:
                            # coloca nesta linha/coluna o primiro item da lista
                            tab[coluna][numLinha] = lstItem.pop(0)

                        # duplica a linha para cada item da lista
                        for item in lstItem:
                            # monta um objeto para cada item
                            newObjLst = {}
                            for nomeCol, valCol in tab.items():
                                if isinstance(valCol[numLinha], list):
                                    newObjLst[nomeCol] = valCol[numLinha].copy()
                                else:
                                    newObjLst[nomeCol] = valCol[numLinha]

                            # duplica a linha para outro item da tabela
                            newObjLst[coluna] = item

                            # removeAttrib do novo objeto as colunas que serão criadas
                            lstKeys = list(newObjLst.keys())
                            for k in lstKeys:
                                if coluna + '.' in k:
                                    del newObjLst[k]

                            _adicionarLinha(newObjLst)

                # se a coluna continha listas de objetos, removeAttrib a coluna
                if flagObj:
                    del tab[coluna]
            # end FOR colunas
        # end WHILE flagContemListas

        # apaga da tabela, começando pelo final, as linhas que tinham listas de objetos
        linToDelete.reverse()
        for num in linToDelete:
            for k, v in tab.items():
                del tab[k][num]

    # se solicitado, removeAttrib os prefixos dos nomes das colunas
    if flagRemovePrefix:
        cols = list(tab.keys())
        newTab = {}
        for idx in range(len(cols)):
            colBefore = cols[idx]
            if '.' in cols[idx]:
                tks = cols[idx].split('.')
                colAfter = cols[idx] = tks[len(tks)-1]
            else:
                colAfter = colBefore

            newTab[colAfter] = tab[colBefore]
        # endfor --

        tab = newTab
    # endif --

    # completa as colunas com menos linhas
    qde = 0
    for valCol in tab.values():
        if len(valCol) > qde:
            qde = len(valCol)

    for valCol in tab.values():
        if len(valCol) < qde:
            for idx in range(qde - len(valCol)):
                valCol.append(None)
            # endfor --
        # endif --
    # endfor --

    # retorna DataFrame
    cols = list(tab.keys())
    return DataFrame(tab, columns=cols)


def json2table(jdata: str, flagExpandValueLists: bool = False, flagExpandObjectLists: bool = False, flagRemovePrefix: bool = False) -> DataFrame:
    """Converte um string com uma lista de objetos Json para um DataFrame. Cada chave é convertida numa coluna e cada objeto da lista numa linha da tabela.

    Args:
        jdata (str): string com objeto Json
        flagExpandValueLists (bool, optional): se True expande os campos com listas de valores, tratando-as como tabelas associadas num CROSS-JOIN
        flagExpandObjectLists (bool, optional): se True expande os campos com listas de objetos, tratando-as como tabelas associadas num CROSS-JOIN
        flagRemovePrefix (bool, optional): se True os prefixos serão removidos dos nomes das colunas

    Returns:
        [pandas.DataFrame]: tabela com os objetos.
    """

    objs = js.loads(jdata)
    return dict2table(objs, flagExpandValueLists, flagExpandObjectLists, flagRemovePrefix)
