"""
    jDocument

    05/Out/2020 - José Carlos Cordeiro

    Este e módulo implementa uma classe para encapsular um documento ou uma lista de documentos Json
    que provê um conjunto de HELPERs para acessar e manipular o seu conteúdo:
    . Encapsular o documento num objeto que fornece serviços para inspecionar e atualizar o documento.
    . Transformar o documento numa tabela DataFrame.

    Um documento json é composto por:
    a) Uma coleção de pares de chave/valor como:
    {
        "chave1" : valor1,
        "chave2" : valor,
        ...
    }

    b) Ou por um Array de documentos como:
    [
        { documento 1},
        { documento 2},
        ...
    ]

    Classes:
        jDocument
        DotDict
        jDotDict
"""

from __future__ import annotations

import datetime as dt
import re
import sys
from copy import deepcopy
import statistics
import numpy
from collections.abc import Sequence

from JsonObjects import jsjson as js
from JsonObjects.helpers import getDocAttributes, str2datetime

# função usada em macros eval()
from JsonObjects.helpers import getDateGroup

CONST_JDATA = 'jdata'
CONST_TYPE_ARRAY = 'Array'
CONST_TYPE_OBJECT = 'Object'
CONST_ERR_ARRAY = 'This Json document must be a List/Array'
CONST_ERR_OBJECT = 'This Json document must be a Object'
CONST_ERR_ITEM = 'The Item must be a jDocument or a Json Dictionary (dict) or a list of dic'


class DotDict(dict):
    def __bool__(self):
        return True

    def __getattr__(self, key):
        try:
            # se o atributo for um dict, retorna o dict
            if isinstance(self[CONST_JDATA][key], dict):
                return DotDict({CONST_JDATA: self[CONST_JDATA][key]})

            # senão retorna o elemento
            else:
                return self[CONST_JDATA][key]

        except KeyError as k:
            raise AttributeError(k)

    def __setattr__(self, key, value):
        self[CONST_JDATA][key] = value
        # self._jdata.set({key, value})

    def __delattr__(self, key):
        try:
            del self[CONST_JDATA][key]
        except KeyError as k:
            raise AttributeError(k)


class jDotDict(dict):
    def __bool__(self):
        return True

    def __getattr__(self, key):
        try:
            at = self[CONST_JDATA][key]

            # se o atributo for um dict, retorna o dict
            if isinstance(at, dict):
                return jDotDict({CONST_JDATA: at})

            # se o atributo for um list
            elif isinstance(at, list):
                # se o primeiro elemento da lista for um dict (lista de documentos), retorna um jDocument com a lista
                # senão retorna o elemento
                return jDocument(at) if len(at) > 0 and isinstance(at[0], dict) else jDotDict({CONST_JDATA: at})

            # senão retorna o elemento
            else:
                return at

        except KeyError as k:
            raise AttributeError(k)

    def __setattr__(self, key, value):
        self[CONST_JDATA][key] = value
        # self._jdata.set({key, value})

    def __delattr__(self, key):
        try:
            del self[CONST_JDATA][key]
        except KeyError as k:
            raise AttributeError(k)


class jDocument(Sequence):
    """
    Representa um documento Json ou uma lista de documentos.
    """

    def __init__(self, jdata=None):
        """
        Returns:
            object:
        """
        super().__init__()

        self._current = -1  # para iterações na classe
        if jdata is None:
            # self._jdata = DotDict({})
            self._jdata = {}
            self._type = CONST_TYPE_OBJECT

        elif isinstance(jdata, dict):
            # self._jdata = DotDict(jdata)
            self._jdata = jdata
            self._type = CONST_TYPE_OBJECT

        elif isinstance(jdata, list):
            self._jdata = jdata
            self._type = CONST_TYPE_ARRAY

        else:
            self._jdata = js.loads(jdata)

            if isinstance(self._jdata, dict):
                # self._jdata = DotDict(self._jdata)
                self._jdata = self._jdata
                self._type = CONST_TYPE_OBJECT
            else:
                self._type = CONST_TYPE_ARRAY
            # endif --
        # endif --

        self._findDocs_lstFilters = []
        self._findDocs_qty = None
        self._findDocs_flagMacros = None

        self._searhDocs_jOrFilters = None
        self._searhDocs_exprFilter = None
        self._searhDocs_qty = None

    def __bool__(self):
        return len(self._jdata.keys()) > 0 if self._type == CONST_TYPE_OBJECT else len(self._jdata) > 0

    def __iter__(self):
        self._current = -1
        return self

    def __next__(self):
        self._current += 1
        if self._current < len(self._jdata):
            return jDocument(self._jdata[self._current]) if self._type == CONST_TYPE_ARRAY else list(self._jdata.values())[self._current]

        # chegou no último, reinicia
        self._current = -1

        raise StopIteration

    def __repr__(self):
        if self._type == CONST_TYPE_ARRAY:
            return f"{__class__.__name__}<{self._type}> : { len(self._jdata) } elements"
        else:
            return f"{__class__.__name__}<{self._type}> : { str(self._jdata) }"

    def __hash__(self):
        return hash(self.getJson(flagPretty=False))

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.__hash__() == other.__hash__()

    def __len__(self):
        if self._type == CONST_TYPE_ARRAY:
            return len(self._jdata)

        return 0

    def __getitem__(self, item):
        return jDocument(self._jdata[item]) if self._type == CONST_TYPE_ARRAY else self.get(item)

    def __setitem__(self, key, value):
        if self._type != CONST_TYPE_OBJECT:
            raise Exception(CONST_ERR_OBJECT)

        self.set({key: value})

        return value

    def __delitem__(self, key):
        if self._type == CONST_TYPE_ARRAY:
            self.removeDocs(index=key)
        else:
            self.removeAttrib(attrib=key)

    def __contains__(self, item):
        return item in (self._jdata.values() if self._type == CONST_TYPE_ARRAY else self._jdata.keys())

    def __reversed__(self):
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        for doc in self._jdata[::-1]:
            yield jDocument(doc)

    @property
    def doc(self) -> DotDict | None:
        """
        Retorna uma referência para o dicinionário cujas chaves podem ser acessadas por meio da notação de pontos
        Sempre retorna o atributo no seu tipo nativo (dict, list, str, etc.)

        Exemplo:
            doc = {'nome': 'maria', {'ocupacao': {'profissao': 'analista'}}}
            print(jDoc.doc.nome)
            print(jDoc.doc.nome.ocupacao.profissao)
            jDoc.doc.nome = 'jose'

        Returns:
            [DotDict]: referência para o dicionário, se o documento for uma lista, então retorna None
        """
        if isinstance(self._jdata, dict):
            return DotDict({CONST_JDATA: self._jdata})

        return None

    @property
    def jdoc(self) -> jDotDict | None:
        """
        Retorna uma referência para o dicionário cujas chaves podem ser acessadas por meio da notação de pontos
        Quando o atributo for uma lista, retorna um jDocument

        Exemplo:
            doc = {'nome': 'maria', {'ocupacao': {'profissao': 'analista'}}}
            print(jDoc.doc.nome)
            print(jDoc.doc.nome.ocupacao.profissao)
            jDoc.doc.nome = 'jose'

        Returns:
            [jDotDict]: referência para o dicionário, se o documento for uma lista, então retorna None
        """
        if isinstance(self._jdata, dict):
            return jDotDict({CONST_JDATA: self._jdata})

        return None

    @property
    def jData(self) -> dict:
        """
        Retorna uma referência para o dicionário que compoem o documento Json.

        Returns:
            [dict]: referência para o dicinionário
        """
        return self._jdata

    @property
    def type(self) -> str:
        """
        Retorna o tipo do documento Json, que pode ser um objeto ou uma lista de objetos

        Returns:
            str: CONST_TYPE_OBJECT para um objeto ou CONST_TYPE_ARRAY para uma lista de objetos
        """
        return self._type

    def getNumOfItems(self, attribute: str = None, defaulf: int = 0) -> int:
        """
        Informa a quantidade de itens em uma lista, que pode ser o próprio do documento ou um atributo do documento.

        Args:
            attribute: nome do atributo, se None será considerado o próprio documento
                       default = None
           defaulf: valor default para retornar no lugar de None

        Returns:
            int: quantidade de elementos na lista
            None: se o atributo não existir ou não for uma lista
        """
        if not attribute:
            at = self._jdata
        else:
            at = self.value(attribute)
            if not at:
                return defaulf
            # endif
        # endif

        if isinstance(at, list):
            return len(at)
        # endif

        return defaulf

    def getAttributes(self, flagDeepDocs: bool = True) -> dict:
        """
        Retorna um dicionário com a lista de atributos contidos nos objetos.

        Args:
            flagDeepDocs (bool, optional): se True coleta também os atributos de documentos aninhados.

        Returns:
            dict: dicionário com os nomes e tipos dos atributos.
        """
        # dicionário onde serão calatalogados os atributos
        attribs = {}

        # se há uma lista de objetos no documento
        if self._type == CONST_TYPE_ARRAY:
            # verifica os atributos de cada um deles
            if len(self._jdata) > 0:
                list(map(lambda obj: getDocAttributes(attribs, obj, flagDeepDocs=flagDeepDocs), self._jdata))
            # endif --

        # senão verifica os atributos do objeto contido no documento
        else:
            getDocAttributes(attribs, self._jdata, flagDeepDocs=flagDeepDocs)
        # endif --

        return attribs

    def getDataType(self, attrib: str) -> str | None:
        """
        Informa o tipo de um atributo.

        Args:
            attrib: nome do atributo

        Returns:
            str: nome do atributo
        """
        val = self.value(attrib)

        if not val:
            return None
        # endif

        if isinstance(val, dict):
            return CONST_TYPE_OBJECT
        # endif

        if isinstance(val, list):
            return CONST_TYPE_ARRAY
        # endif

        refind = re.compile(r".*'(.*)'.*")  # RegEx para extrair o tipo do atributo
        gp = refind.search(str(type(val)))
        tp = gp.group(1)

        if '.' in tp:
            tk = tp.split('.')
            tp = tk[-1]
        # endif

        return tp

    def isArray(self) -> bool:
        """
        Retorna True se o documento Json for uma lista, senão retorna False.

        Returns:
            bool: True se o documento for uma lista.
        """
        return self._type == CONST_TYPE_ARRAY

    def isObject(self) -> bool:
        """
        Retorna True se o documento Json for um objeto, senão retorna False.

        Returns:
            bool: True se o documento for um objeto.
        """
        return self._type == CONST_TYPE_OBJECT

    def getJson(self, flagPretty: bool = False, ensure_ascii: bool = False) -> str:
        """
        Converte o objeto Json num string contendo o documento Json correspondente.

        Args:
            flagPretty: Gera um documento 'bonitinho' , com identação
            ensure_ascii: Transforma todos os caracters em ASCII (para requests precisa ser TRUE)

        Returns:
            str: documento Json
        """
        if flagPretty:
            return js.dumps(self._jdata, flagPretty=flagPretty, ensure_ascii=ensure_ascii)

        return js.dumps(self._jdata, flagPretty=flagPretty, ensure_ascii=ensure_ascii)

    def value(self, attribute=None, defaultValue: any = None, flagRaiseError: bool = False) -> any:
        """
        Retorna um atributo dentro de um objeto Json no seu formato nativo (dict, list, str, int, etc.)\n
        Se não for informado nenhum 'attribute' então retorna o próprio objeto.

        Por exemplo:\n
            'pessoa.endereco.rua' -> retorna o atributo 'rua' do objeto 'endereco' contido em 'pessoa'\n
            'pessoa.enderecos[1].rua' -> retorna o atributo 'rua' do segundo elemento da lista 'enderecos' contida em 'pessoa'\n
            'Array[2].nome' -> quando o documento Json contem uma lista usa-se a palavra CONST_TYPE_ARRAY mais o indide, neste caso retorna o atributo 'nome' do item 2 da lista\n
        Em caso de erro retorna uma string '*** Error descriton'

        Args:
            attribute (str | list): nome do atributo ou lista de nomes
            defaultValue (any) : se o atributo não existir, retorna este valor
            flagRaiseError (bool) : se True gera erro caso o atributo não existea, senão retorna o defaultValue

        Returns:
            any: se o documento Json for um objeto e o atributo for um string, retorna o valor correspondente ao atributo.
            dict: se o documento Json for um objeto e for pedido uma lista de atributos, retorna um dicionário com 'attribute': 'valor'.
            list: se o documento Json for uma lista de objetos, uma lista de valores.
        """
        # se não foi informado nenhum atributo, retorna o próprio dicionário ou array
        if not attribute:
            return self._jdata
        # endif

        # se foi informado um único atributo
        if isinstance(attribute, str):
            # se o documento Json é uma lista
            if self._type == CONST_TYPE_ARRAY:
                # se o atributo procurado não começa com CONST_TYPE_ARRAY
                if not attribute.startswith(CONST_TYPE_ARRAY):
                    if '[' in attribute:
                        # tratamento especial para listas
                        #   itens[10].nome --> pega o valor do atributo "nome" do objeto 10 da lista "itens"
                        #   itens[nome=maria].idade --> pega o valor do atributo "idade" do objeto cujo
                        #                               atributo "nome" é igual a "maria" da lista "itens"
                        # nome da lista - string antes da '['
                        lst = attribute.split('[')[0]
                        # pega indice ou condição dentro das chaves '[...]'
                        pont = attribute.split('[')[1].replace(']', '')
                        if pont.isdigit():
                            # indice numérico
                            idx = int(pont)
                            return self._jdata[lst][idx]
                        else:
                            # condição
                            expr = attribute.split('=')
                            p1 = expr[0].strip()
                            p2 = expr[1].strip()
                            if lst == CONST_TYPE_ARRAY:
                                return self.findOneDoc({p1: p2})
                            else:
                                return self.get(lst).findOneDoc({p1: p2})
                        # endif --
                    else:
                        # coleta uma lista de valores, um para cada objeto da lista
                        return [jDocument(obj).value(attribute, defaultValue) for obj in self._jdata]
                    # endif --
                # endif --
            # endif --

            # o documento Json é um documento único ou
            # é uma lista e o atributo procurado começa com 'Array'
            # coleta um valor, correspondente ao atributo informado
            if '.' not in attribute and '[' not in attribute:
                # atributo único, sem lista nem subdocumento
                try:
                    return self._jdata[attribute]

                except Exception:
                    if flagRaiseError:
                        raise Exception(f"*** {sys.exc_info()[0]}")
                    else:
                        return defaultValue
                    # endif --
                # end_except --
            # endif --

            k = attribute.split('.')
            val = defaultValue
            dic = self._jdata

            try:
                for tk in k:
                    if '[' in tk:
                        # tratamento especial para listas
                        #   itens[10].nome --> pega o valor do atributo "nome" do objeto 10 da lista "itens"
                        #   itens[nome=maria].idade --> pega o valor do atributo "idade" do objeto cujo
                        #                               atributo "nome" é igual a "maria" da lista "itens"
                        # nome da lista - string antes da '['
                        lst = tk.split('[')[0]
                        # pega indice ou condição dentro das chaves '[...]'
                        pont = tk.split('[')[1].replace(']', '')
                        if pont.replace('-', '').isnumeric():
                            # indice numérico
                            idx = int(pont)
                            val = dic[idx] if lst == CONST_TYPE_ARRAY else dic[lst][idx]
                        else:
                            # condição
                            expr = tk.split('[')[1].split('=')
                            p1 = expr[0].strip()
                            p2 = expr[1].replace(']', '').strip()
                            val = jDocument(dic).findOneDoc({p1: p2}).value() if lst == CONST_TYPE_ARRAY else jDocument(dic[lst]).findOneDoc({p1: p2}).value()
                        # endif --
                    else:
                        val = dic[tk]
                    # endif --
                    dic = val
                # endfor --
            except Exception:
                if flagRaiseError:
                    raise Exception(f"*** {sys.exc_info()[0]}")
                else:
                    val = defaultValue
                # endif --
            # end_except --

            return val
        # endif --

        # se foi informado uma lista de atributos
        if isinstance(attribute, list):
            # se o documento Json é um objeto
            if self._type == CONST_TYPE_OBJECT:
                # monta um dicionário com o valor de cada atributo
                dic = {at: self.value(at, defaultValue) for at in attribute}

                return dic

            # o documento Json é uma lista de documentos
            else:
                # monta uma lista os valores dos atributos para cada objeto
                lst = [
                    {at: jDocument(obj).value(at, defaultValue) for at in attribute}
                    for obj in self._jdata
                ]
                return lst
            # endif --
        # endif --

        return None

    def exists(self, attribute: str) -> bool:
        """
        Informa se um atributo existe no documento

        Args:
            attribute: nome do atributo

        Returns:
            bool: True se existir ou False caso contrário
        """
        return self.value(attribute, None, flagRaiseError=False)

    def get(self, attribute: str, defaultValue=None, flagRaiseError: bool = False, flagReturnEmptyListAsDoc: bool = False) -> any:
        """
        Se o documento Json for um objeto, retorna o valor de um atributo do objeto Json.
        Se o documento for uma lista de objetos, retorna um jDocument com a lista de valores.\n
        Por exemplo:\n
            'Pessoa.Endereco.Rua' -> retorna o atributo 'Rua' do objeto 'Endereco' contido em 'Pessoa'\n
            'Pessoa.Enderecos[1].Rua' -> retorna o atributo 'Rua' do segundo elemento da lista 'Enderecos' contida em 'Pessoa'\n
        Em caso de erro retorna uma string '*** Error descriton'

        Args:
            attribute (str): chave
            defaultValue (any) : se a chave não existir, retorna este valor
            flagRaiseError (bool) : se True gera erro caso o atributo não existea, senão retorna o defaultValue
            flagReturnEmptyListAsDoc (bool) : se True, ao retornar uma lista vazia, converte a lista pára jDocument

        Returns:
            any: objeto da lista ou valor correspondente à chave (str, int, double, etc.)
        """
        val = self.value(attribute, defaultValue, flagRaiseError)

        if isinstance(val, dict):
            return jDocument(val)

        if isinstance(val, list):
            if len(val) > 0 and isinstance(val[0], dict):
                return jDocument(val)
            # endif

            if not val and flagReturnEmptyListAsDoc:
                return jDocument(val)
            # endif
        # endif --

        return val

    def set(self, values: dict) -> any:
        """
        Adiciona ou atualiza um atributo no documento Json. Se o documento for uma lista, atualiza o atributo para todos os objetos da lista.
        jObj.set({'nome': 'maria', {'idade': 10}})

        Args:
            values (dict): dicionário com os nomes dos atributos a serem adicionados e seus respectivos valores

        Returns:
            any: o valor do último atributo que foi adicionado ou atualizado
        """
        # se o documento Json for um objeto
        if self._type == CONST_TYPE_OBJECT:
            if len(values.keys()) == 0:
                return None
            # endif

            returnAt = ''

            # para cada atributo na lista de atribuições
            for at in values:
                # remove caracteres inválidos
                returnAt = at

                # se houver documento dentro de documento
                if '.' in at:
                    lstAttrib = at.split('.')
                    subAt: str = at.replace(lstAttrib[0] + '.', '')

                    if not self.get(lstAttrib[0]):
                        # cria o subdocumento se não existir
                        self.set({lstAttrib[0]: {}})
                    # endif

                    self.get(lstAttrib[0]).set({subAt: values[at]})

                else:
                    if isinstance(values[at], jDocument):
                        # se estivermos atribuindo um documento Json, transforma no valor nativo
                        self._jdata[at] = values[at].value()
                    else:
                        self._jdata[at] = values[at]
                    # endif --
                # endif --
            # endfor --

            if isinstance(values[returnAt], dict):
                return jDocument(values[returnAt])
            else:
                return values[returnAt]

        else:
            # senão, é uma lista
            # adiciona/atualiza o atributo em todos os objetos da lista
            for obj in self._jdata:
                obj.set(values)
            # endif --

            return None
        # endif --

    def clear(self):
        """
        Limpa a lista de objetos.
        """
        self._jdata.clear()

    def item(self, idx: int) -> any:
        """
        Retorna um objeto dentro de uma lista, se o conteúdo do documento Json não for uma lista gera erro\n

        Args:
            idx (int): índice da lista

        Returns:
            jDocument: um objeto da lista de documentos
        """
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        return jDocument(self._jdata[idx])

    def removeAttrib(self, attrib: str = None, attribs: list = None) -> int:
        """
        Remove um atributo do documento Json. Se o documento for uma lista, removeAttrib de todos os objetos da lista.

        Args:
            attrib (str): nome do atributo, se informado o atributo com este nome será removido
            attribs (list): lista de nomes de atributos, se informado todos eles serão removidos

        Returns:
            int: quantidade de atributos removidos
        """
        # se foi informado uma lista de nomes de atributos
        q = 0
        if attribs:
            for at in attribs:
                q += self.removeAttrib(attrib=at)
            # endfor --

            return q
        # endif --

        # se o documento Json for um objeto
        if self._type == CONST_TYPE_OBJECT:

            q = 0
            if attrib:
                # se foi informado o nome de um atributo específico
                if '.' in attrib:
                    # se foi informado um atributo de um subObjeto
                    at = attrib.split('.')
                    lastAt = at.pop()
                    obj = self

                    for atSub in at:
                        obj = obj.get(atSub)
                    # endfor --

                    q += obj.removeAttrib(lastAt)

                else:
                    # senão, é um atributo do próprio objeto
                    if attrib in self._jdata:
                        # se o atributo existe neste objeto, removeAttrib
                        del self._jdata[attrib]
                        q = 1
                    # endif --
                # endif --
            # endif --

            return q
        # endif --

        # senão, se é um ARRAY
        q = 0
        # for obj in self._jdata:
        for obj in self:
            # removeAttrib o atributo em cada objeto da lista
            q += obj.removeAttrib(attrib=attrib, attribs=attribs)
        # endfor --

        return q

    def addDoc(self, item) -> jDocument:
        """
        Adiciona um objeto ou uma lista de objetos ao documento Json, que precisa ser uma lista de objetos.

        Args:
            item (jDocument | dict | list): dict ou objeto (do tipo jDocument) a ser adicinado na lista.

        Returns:
            jDocument: retorna o objeto jDocument que foi adicionado
        """
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        if isinstance(item, jDocument):
            obj = item
            if item.type == CONST_TYPE_ARRAY:
                # item é um jDocument com uma lista de objetos (list)
                self._jdata.extend(item.value())
            else:
                # item é um jDocument com um objeto (dict)
                self._jdata.append(item.value())
            # endif --

        elif isinstance(item, dict):
            obj = jDocument(item)
            self._jdata.append(item)

        elif isinstance(item, list):
            obj = jDocument(item)
            self._jdata.extend(item)

        else:
            raise Exception(CONST_ERR_ITEM)
        # endif --

        return obj

    def removeOneDoc(self, filters: any = None) -> int:
        """
        Remove todos os objetos que corresponderem a um filtro informado (uma coleção de chave/valor).
        O documento Json precisa ser uma lista de objetos.
        A pesquisa é 'Case Insensitive' e trata caracteres acentuados como não acentuados.

        Args:
            filters (dict | list):  conjunto de condições para validar cada objeto da coleção\n
                                    dict -> dicionário com os nomes dos atributos e expressão regular correspondentes como {'nome' : 'maria'}, os objetos que derem match serão removidos
                                    list -> lista de condições (dict), se o objeto corresponder a uma das condições da lista será removido

        Returns:
            int: quantidade de objetos removidos
        """
        return self.removeDocs(filters=filters, qty=1)

    def removeDocs(self, array: str = None, index: int = None, filters: any = None, qty: int = None) -> int:
        """
        Remove todos os objetos que corresponderem a um filtro informado (uma coleção de chave/valor).
        O documento Json precisa ser uma lista de objetos.
        A pesquisa é 'Case Insensitive' e trata caracteres acentuados como não acentuados.

        Args:
            array (int): nome do atributo com a lista de itens, removeAttrib um item do documento Json, que precisa ser uma lista
            index (int): se informado, corresponde ao índice da lista correspondente ao objeto a ser eliminado
            filters (dict | list):  conjunto de condições para validar cada objeto da coleção\n
                                    dict -> dicionário com os nomes dos atributos e expressão regular correspondentes como {'nome' : 'maria'}, os objetos que derem match serão retornados
                                    list -> lista de condições (dict), se o objeto corresponder a uma das condições da lista será retornado
            qty (int): indica a quantidade máxima de objetos a serem removidos, default igual a todos

        Returns:
            int: quantidade de objetos removidos
        """
        # se for informado o nome de atributo com lista de documentos
        if array:
            # foi informado o nome da coleção
            # se foram informadas condições
            if filters:
                # pega o objeto com a lista de documentos
                lst = self.get(array)
                q = lst.removeDocs(filters=filters, qty=qty)

            elif index:
                # se foi informado um indice
                # pega o objeto associado ao índice
                lst = self.get(array)
                q = lst.removeDocs(index=index)

            else:
                # senão está sem índice nem condição, limpa a lista
                lst = self.value(array)
                q = len(lst)
                del lst[:]
            # endif --

            return q
        # endif --

        if index is not None:
            # NÃO foi informado o nome da coleção
            # foi informado um indice
            if self._type != CONST_TYPE_ARRAY:
                raise Exception(CONST_ERR_ARRAY)

            del self._jdata[index]

            return 1
        # endif --

        # SE foi informada um conjunto de condições
        if filters:
            # NÃO foi informado o nome da coleção
            if self._type != CONST_TYPE_ARRAY:
                raise Exception(CONST_ERR_ARRAY)
            # endif --

            # gera a lista dos elementos a remover
            jDocsToRemove = self.findDocs(filters, qty)

            lstRemove = [] if not jDocsToRemove else jDocsToRemove.value()

            # exclui os elementos listados
            for i in reversed(range(len(lstRemove))):
                idel = lstRemove[i - 1]
                self._jdata.remove(idel)
            # endfor --

            return len(lstRemove)
        # endif --

        if index is None:
            # NÃO foi informado o nome da coleção
            # NÃO foi informado um indice
            if self._type != CONST_TYPE_ARRAY:
                raise Exception(CONST_ERR_ARRAY)

            q = len(self._jdata)
            self._jdata.clear()

            return q
        # endif --

        return 0

    def findOneDoc(self, filters: any, flagMacros: bool = False) -> jDocument | None:
        """
        Localiza o primeiro objeto que corresponder a um filtro informado (uma coleção de chave/valor).
        O documento Json precisa ser uma lista de objetos.
        A pesquisa é 'Case Insensitive' e trata caracteres acentuados como não acentuados.

        Args:
            filters (dict | list):  conjunto de condições para validar cada objeto da coleção\n
                                    dict -> dicionário com os nomes dos atributos e expressão regular correspondentes como {'nome' : 'maria'}, os objetos que derem match serão retornados
                                    list -> lista de condições (dict), se o objeto corresponder a uma das condições da lista será retornado
            flagMacros: se TRUE então valida macros dentro do filtro (IN, NIN, RE, etc.)

        Returns:
            jDocument: jDocument com documento Json contendo documento localizado, ou None se não localizar nenhum.
        """
        jObj = self.findDocs(filters, qty=1, flagMacros=flagMacros)

        if jObj:
            return jObj.item(0)
        # endif --

        return None

    @staticmethod
    def _findDocs_TestAttrib(rule, val) -> bool:
        if isinstance(rule, str):
            if len(rule) <= 4:
                return rule == val
            # endif --

            if rule[2] != ':' and rule[3] != ':':
                # comparação simples de valores
                return rule == val
            # endif --

            if rule.startswith('IN:'):
                # se for um IN, valida como tal
                return val in rule[3:]

            elif rule.startswith('NIN:'):
                # se for um NIN (not in), valida como tal
                return val not in rule[4:]

            elif rule.startswith('CT:'):
                # se for um CT (contains), valida como tal
                return rule[3:] in val

            elif rule.startswith('NCT:'):
                # se for um NCT (not contains), valida como tal
                return rule[4:] not in val

            elif rule.startswith('RE:'):
                return True if re.search(rule[3:], val, flags=re.IGNORECASE) else False
            # endif --
        # endif --

        return rule == val

    def _findDocs_TestDoc(self, docDic: dict) -> bool:
        if self._findDocs_qty and self._findDocs_qty < 1:
            # está buscando uma quantidade específica de elementos
            # já localizou a quantidade necessária, ignora os demais
            return False
        # endif

        for conds in self._findDocs_lstFilters:
            for at, ruleExpr in conds.items():
                if '.' in at:
                    # a regra se aplica a um atributo de um subdocumento
                    jDoc = jDocument(docDic)
                    valAttr = jDoc.get(at, None)
                else:
                    # trata-se de um atributo do documento raiz
                    # valAttr = docDic[at] if at in docDic else None
                    valAttr = docDic.get(at, None)
                # endif --

                # se o atributo for uma lista ou um dicionário (objeto) então transforma em string
                if isinstance(valAttr, dict) or isinstance(valAttr, list):
                    valAttr = str(valAttr)
                # endif --

                if not self._findDocs_flagMacros:
                    if ruleExpr != valAttr:
                        return False
                    # endif --

                elif not jDocument._findDocs_TestAttrib(ruleExpr, valAttr):
                    return False
                # endif --
            # endfor --
        # endfor --

        if self._findDocs_qty:
            self._findDocs_qty -= 1
        # endif

        return True

    def findDocs(self, filters: any, qty: int = None, flagMacros: bool = False) -> jDocument | None:
        """
        Localiza todos os objetos que corresponderem a um filtro informado (uma coleção de chave/valor).
        O documento Json precisa ser uma lista de objetos.
        A pesquisa é 'Case Insensitive' e trata caracteres acentuados como não acentuados.

        Args:
            filters (dict | list):  conjunto de condições para validar cada objeto da coleção\n
                                    dict -> dicionário com os nomes dos atributos e as condições pertinentes como:
                                            {'nome': 'maria'} --> comparação simples, os 'nomes' iguais a 'maria' darão match
                                            {'nome': 'RE:^mar'} --> compara com RegExp, os 'nomes' que começarem com 'mar' dão match
                                            {'nome': 'IN:mar'} --> compara usando IN, os 'nomes' que contêm 'mar' dão match
                                    list -> lista de condições (dict), se o objeto corresponder a uma das condições da lista será retornado
            qty (int): indica a quantidade máxima de objetos a serem retornados, default igual a todos
            flagMacros: se TRUE então valida macros dentro do filtro (IN, NIN, RE, etc.)

        Returns:
            jDocument: jDocument com documento Json contendo a lista de documentos localizados.
                       ou None se não localizar nenhum
        """
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        self._findDocs_flagMacros = flagMacros

        # se foi passado um dicionário de condições monta uma lista com apenas essa condição senão considera esta lista de condições
        self._findDocs_lstFilters = [filters] if isinstance(filters, dict) else filters

        # percorre a lista de objetos
        self._findDocs_qty = qty

        findList = list(filter(self._findDocs_TestDoc, self._jdata))

        self._findDocs_lstFilters = None
        self._findDocs_qty = None

        if not findList:
            return None
        # endif --

        return jDocument(findList)

    def findAttribDocs(self, lstAttrib: list, qty: int = None) -> jDocument:
        """
        Monta uma lista dos objetos que contém um certo conjunto de atributos

        Args:
            lstAttrib (list): lista com os nomes dos atributos
            qty (int): indica a quantidade máxima de objetos a serem retornados, default igual a todos

        Returns:
            jDocument: jDocument com documento Json contendo a lista de documentos localizados.
        """
        filters = {at: '.*' for at in lstAttrib}

        return self.findDocs(filters, qty)

    def findAnyDocs(self, lstFilters: list, qty: int = None) -> jDocument:
        """
        Procura um texto dentro de cada documento da lista e retorna aqueles que corresponderem ao critério.
        A pesquisa é 'Case Insensitive' e trata caracteres acentuados como não acentuados.

        Args:
            lstFilters (list): lista com uma ou mais 'regex' com o texto a ser procurado
            qty (int): indica a quantidade máxima de objetos a serem retornados, default igual a todos

        Returns:
            jDocument: jDocument com documento Json contendo a lista de documentos localizados.
        """
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        findList = []
        q = 0

        # pesquisa cada objeto do documento
        for obj in self._jdata:
            jObj = jDocument(obj)

            # testa cada regexp contra a documento
            for ft in lstFilters:
                # se deu match, separa este documento e vai para o próximo
                s = jObj.getJson(flagPretty=False)

                if re.search(ft, s, flags=re.IGNORECASE):
                    findList.append(obj)
                    break
                # endif --
            # endfor --

            q += 1

            if qty and q > qty:
                break
            # endif --
        # endfor --

        return jDocument(findList)

    def sortDocs(self, attribute: str | dict | list) -> jDocument:
        """
        Ordena a lista de documentos Json.

        Args:
            attribute (str|dict|list): Nome do atributo ou dicionário com "atributo:ordem" ou uma lista de atributos (com dicionários).
                                   Se for igual a 1 a ordem será crescente, se -1 será descrescente.


        Return:
            self: retona o próprio documento Json já ordenado
        """
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        # se foi informado uma lista de atributos
        if isinstance(attribute, list):
            list(map(lambda at: self.sortDocs(at), reversed(attribute)))
            return self
        # endif --

        if isinstance(attribute, str):
            # foi informado apenas o nome do atributo
            # self._jdata = sorted(self._jdata, key=lambda i: i[attribute], reverse=False)
            self._jdata.sort(key=lambda e: (not e[attribute], e[attribute]))

        else:
            # foi informado um dicionário com o nome do atributo e a sequência (0 ou 1)
            for sortAttrib, order in attribute.items():
                # self._jdata = sorted(self._jdata, key=lambda i: i[sortAttrib], reverse=(False if order == 1 else True))
                self._jdata.sort(reverse=(True if order != 1 else False), key=lambda e: (not e[sortAttrib], e[sortAttrib]))
            # endfor --
        # endif --

        return self

    def __deepcopy__(self, memodict=None) -> jDocument:
        return self.clone()

    def clone(self) -> jDocument:
        """
        Cria uma cópia do documento.

        Returns:
            jDocument: cópia do documento
        """
        return jDocument(deepcopy(self._jdata))

    def copyFrom(self, jDoc: jDocument):
        """
        Copia todos os atributos do documento de um outro documento, fornecido como parâmetro

        Args:
            jDoc: documento do qual serão copiados os atributos

        """
        self.clear()
        attribDict = jDoc.getAttributes(False)
        for at, tp in attribDict.items():
            value = jDoc.value(at)
            if tp == CONST_TYPE_OBJECT:
                self.set({at: value.copy()})
            elif CONST_TYPE_ARRAY in tp:
                self.set({at: list(value)})
            else:
                self.set({at: value})
            # endif --
        # endfor --

    def searchOneDoc(self, jFilters: jDocument) -> jDocument:
        return self.searchDocs(jFilters, qty=1)

    def _searchDocs_TestDoc(self, docdic) -> bool:
        if self._searhDocs_jOrFilters and self.testDoc(jDocument(docdic), self._searhDocs_jOrFilters):
            return True
        # endif --

        if self._searhDocs_exprFilter:
            # 'jDoc' é uma variável para expressão de validação
            jDoc = jDocument(docdic)
            if eval(self._searhDocs_exprFilter):
                return True
            # endif --
        # endif --

        return False

    def searchDocs(self, jOrFilters: jDocument, exprFilter: str = None, qty: int = None) -> jDocument:
        """
        Pesquisa a lista de documento e retorna aqueles que corresponderem a um conjunto de condições avançadas de banco de dados.
        Args:
            jOrFilters: conjunto de condições na forma "attributo operador valor", por exemplo
                [
                    {
                        and:
                            [
                                {attribute: 'nome', operator: 'eq', value: 'jose'},
                                {attribute: 'idade', operator: 'gt', value: 20}
                            ]
                    {,
                    {
                        and:
                        [
                            {attribute: 'nome', operator: 'eq', value: 'maria'},
                            {attribute: 'idade', operator: 'ltqe', value: 30}
                        ]
                    }
                ]

                equivale a: (nome = 'jose' AND idade > 20) OR (nome = 'maria' AND idade <= 30)

                As opções para operador são:
                    eq = equal
                    dif = not equal
                    lt = less than
                    lteq = less than or equal
                    gt = greater than
                    gteq = greater than or equal
                    ct = contain
                    nct = not contain
                    re = RegExp
                    in = contido numa lista
                    nin = NÃO contido numa lista

                Se o nome do campo for "all" então será feita uma pesquisa de um texto dentro do documento JSON
                    ** Neste caso, são aceitos somente os operadores "contain" e "not contain"

            exprFilter: expressão Python com a condição de pesquisa, para referenciar os atributos do documento
                  utiliza-se a variável "jDoc". Por exmeplo, "jDoc.get('Idade') > 18"
            qty: indica a quantidade máxima de registros desejada no resultado

        Returns:
            jDocument: lista de documentos localizados
        """
        if self._type != CONST_TYPE_ARRAY:
            if self.testDoc(jDocument(self._jdata), jOrFilters):
                return jDocument([jDocument(self._jdata)])
            else:
                return jDocument([])
        # endif --

        self._searhDocs_jOrFilters = jOrFilters
        self._searhDocs_exprFilter = exprFilter
        self._searhDocs_qty = qty

        findList = list(filter(self._searchDocs_TestDoc, self._jdata))

        self._searhDocs_jOrFilters = None
        self._searhDocs_exprFilter = None
        self._searhDocs_qty = None

        return jDocument(findList)

    @staticmethod
    def testDoc(jDoc: jDocument, jOrFilters: jDocument) -> bool:
        """
        Testa um documento contra um filtro de pesquisa e informa se correspomde ou não.

        Args:
            jDoc: documento a ser testado.
            jOrFilters: filtro.

        Returns:
            bool: TRUE se corresponder ou FALSE caso contrário
        """
        flagFind = True

        # testa alternativas de condições (OR) - basta uma ser positiva que o filtro dá match
        for jAndFilters in jOrFilters:
            # testa um grupo de condições (AND) - basta uma ser negativa que invalida o filtro
            for jFilter in jAndFilters.get('And'):
                filterAttrib = jFilter.get('Attribute')

                value = jFilter['Value']
                attribVal = jDoc.get(filterAttrib)
                oper = jFilter.get('Operator')

                if filterAttrib == 'all':
                    # pesquisa o texto informado dentro do documento JSON
                    txt = jDoc.getJson(flagPretty=False)
                    if oper == "ct":
                        flagFind = value.lower() in txt.lower()
                    elif oper == "nct":
                        flagFind = value.lower() not in txt.lower()
                    else:
                        raise Exception(f"Err: the perator {oper} may not be used to search this type of document!")

                else:
                    # pesquisa o valor num campo do documento
                    if isinstance(value, dt.datetime) and not isinstance(attribVal, dt.datetime):
                        # valor do filtro é 'datetime' mas valor do atributo é 'string'
                        attribVal = str2datetime(attribVal)

                    if isinstance(value, str):
                        value = value.lower()

                        if isinstance(attribVal, str):
                            attribVal = attribVal.lower()
                        # endif --
                    # endif --

                    if oper == "eq":  # igual a
                        if not value:
                            flagFind = (attribVal is None)
                        else:
                            flagFind = (attribVal == value)
                        # endif --

                    elif oper == "dif":  # diferente
                        if not value:
                            flagFind = (attribVal is not None)
                        else:
                            flagFind = (attribVal != value)
                        # endif --

                    elif not value or not attribVal:
                        flagFind = False

                    else:
                        match oper:
                            case "lteq":  # menor que ou igual a
                                flagFind = (attribVal <= value)

                            case "gteq":  # maior que ou igual a
                                flagFind = (attribVal >= value)

                            case "lt":  # menor que
                                flagFind = (attribVal < value)

                            case "gt":  # maior que
                                flagFind = (attribVal > value)

                            case "ct":  # contém
                                flagFind = (str(value) in str(attribVal))

                            case "nct":  # NÃO contém
                                flagFind = (str(value) not in str(attribVal))

                            case "in":  # contido numa lista
                                flagFind = attribVal in value

                            case "nin":  # NÃO contido numa lista
                                flagFind = attribVal not in value

                            case "RegExp":  # expressão regular
                                flagFind = (re.search(value, attribVal, flags=re.IGNORECASE) is None) if isinstance(attribVal, str) else False

                            case _:
                                raise Exception(f"Invalid operator '{oper}'")
                        # endmatch --
                    # endif --
                # endif --

                if not flagFind:
                    # basta uma condição ser negativa que invalida o AND filtro
                    break
            # endfor --

            if flagFind:
                break
        # endfor --

        return flagFind

    def _getListOfValues(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> list:
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        if lstFilter:
            jList = self.findDocs(lstFilter)

        elif jFilter or exprFilter:
            jList = self.searchDocs(jOrFilters=jFilter, exprFilter=exprFilter)

        else:
            jList = jDocument(self._jdata)
        # endif --

        if not jList:
            return []
        # endif --

        lstValues = [jDoc.get(attrib) for jDoc in jList if jDoc[attrib]]

        return lstValues

    def count(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return len(lstValues)

    def sum(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return sum(lstValues) if len(lstValues) > 0 else None

    def min(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return min(lstValues) if len(lstValues) > 0 else None

    def max(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return max(lstValues) if len(lstValues) > 0 else None

    def mean(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return statistics.mean(lstValues) if len(lstValues) > 0 else None

    def mode(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return statistics.mode(lstValues) if len(lstValues) > 0 else None

    def median(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return statistics.median(lstValues) if len(lstValues) > 0 else None

    def median_low(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return statistics.median_low(lstValues) if len(lstValues) > 0 else None

    def median_high(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return statistics.median_high(lstValues) if len(lstValues) > 0 else None

    def median_grouped(self, attrib: str, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return statistics.median_grouped(lstValues) if len(lstValues) > 0 else None

    def quantile(self, attrib: str, quantile: float, method: str = 'linear', lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return numpy.quantile(lstValues, quantile, method=method) if len(lstValues) > 0 else None

    def ocorrences(self, attrib: str, valueToCount, lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> int | None:
        lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
        return lstValues.count(valueToCount) if len(lstValues) > 0 else None
