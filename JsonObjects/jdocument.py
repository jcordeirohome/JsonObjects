"""
jDocument

2022/january/1 - Jose Carlos Cordeiro Martins

The jDocument class allows you to encapsulate a json document with a dictionary (dict) or a list of dictionary (list).
In this way, it is possible to perform several operations on the document to access, query and filter json data.

Classes:
    jDocument
"""

from __future__ import annotations

import datetime as dt
import re
import sys
from copy import deepcopy
import statistics
# import numpy
from collections.abc import Sequence

from JsonObjects import jsjson as js
from JsonObjects.helpers import getDocAttributes, str2datetime

CONST_JDATA = 'jdata'
CONST_TYPE_ARRAY = 'Array'
CONST_TYPE_OBJECT = 'Object'
CONST_ERR_ARRAY = 'This Json document must be a List/Array'
CONST_ERR_OBJECT = 'This Json document must be a Object'
CONST_ERR_ITEM = 'The Item must be a jDocument or a Json Dictionary (dict) or a list of dic'


class jDocument(Sequence):
    """
    Representa um documento Json ou uma lista de documentos.
    """

    def __init__(self, jdata=None):
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
        """
        Depending on the json type, it has the default behavior of "dict" or "list".
        """
        return len(self._jdata.keys()) > 0 if self._type == CONST_TYPE_OBJECT else len(self._jdata) > 0

    def __iter__(self):
        """
        Depending on the json type, it iterates over the "dict" keys or over the "list" elements.
        """
        self._current = -1
        return self

    def __next__(self):
        """
        Depending on the json type, it iterates over the "dict" keys or over the "list" elements.
        """
        self._current += 1
        if self._current < len(self._jdata):
            return jDocument(self._jdata[self._current]) if self._type == CONST_TYPE_ARRAY else list(self._jdata.values())[self._current]

        # reached the last element, restart
        self._current = -1
        raise StopIteration

    def __repr__(self):
        """
        For a json containing a dict, it returns the str() of the dictionary; for a list it returns the type and number of elements.
        """
        if self._type == CONST_TYPE_ARRAY:
            return f"{__class__.__name__}<{self._type}> : {len(self._jdata)} elements"
        else:
            return f"{__class__.__name__}<{self._type}> : {str(self._jdata)}"

    def __hash__(self):
        """
        Converts json to string and returns the "hash" of this string.
        """
        return hash(self.getJson(flagPretty=False))

    def __eq__(self, other):
        """
        Depending on the json type, it has the default behavior of "dict" or "list".
        """
        return self.__class__ == other.__class__ and self.__hash__() == other.__hash__()

    def __len__(self):
        """
        Depending on the json type, it returns the number of "dict" keys or "list" elements.
        If the document is empty, it returns 0.
        """
        if self._type == CONST_TYPE_ARRAY:
            return len(self._jdata)

        return len(self._jdata.keys())

    def __getitem__(self, item):
        """
        Depending on the json type, it returns the value associated with a "dict" key or a "list" item.
        """
        return jDocument(self._jdata[item]) if self._type == CONST_TYPE_ARRAY else self.get(item)

    def __setitem__(self, key, value):
        """
        Depending on the json type, it updates the value of a "dict" key or a "list" item.
        """
        if self._type == CONST_TYPE_ARRAY:
            self._jdata[key] = value
        else:
            self.set({key: value})

        return value

    def __delitem__(self, item):
        """
        Depending on the type of json, it removes a key from "dict" or an element from "list".
        """
        if self._type == CONST_TYPE_ARRAY:
            self.removeDocs(position=item)
        else:
            self.removeAttrib(attribute=item)

    def __contains__(self, item):
        """
        Depending on the json type, it informs if a key exists in the "dict" or if an element exists in the "list".
        """
        return item in (self._jdata.values() if self._type == CONST_TYPE_ARRAY else self._jdata.keys())

    def __reversed__(self):
        """
        Inverts the sequence of the elements of a "list", returns an error if the json is a "dict".
        """
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        for doc in self._jdata[::-1]:
            yield jDocument(doc)

    def __deepcopy__(self, memodict=None) -> jDocument:
        """
        Creates a clone of the json document by calling the clone() method.
        """
        return self.clone()

    @property
    def doc(self) -> DotDict | jDocument:
        """
        Returns a reference to the dictionary whose keys can be accessed using the dot notation.

        Example:
            doc = {'nome': 'maria', {'ocupacao': {'profissao': 'analista'}}}
            print(jDoc.doc.name)
            print(jDoc.doc.name.ocupation.name)
            jDoc.doc.name = 'jose'

        Returns:
            [DotDict]: for dict json returns a reference to the document
        """
        if isinstance(self._jdata, dict):
            return DotDict({CONST_JDATA: self._jdata})

        return jDocument(self._jdata)

    @property
    def jdoc(self) -> jDotDict | jDocument:
        """
        Returns a reference to the dictionary whose keys can be accessed using the dot notation.

        Example:
            doc = {'nome': 'maria', {'ocupacao': {'profissao': 'analista'}}}
            print(jDoc.jdoc['name'])
            print(jDoc.jdoc['name.ocupation.name'])
            jDoc.jdoc['name'] = 'jose'

        Returns:
            [jDotDict]: for dict json returns a reference to the document
        """
        return self.doc

    @property
    def jData(self) -> dict:
        """
        Returns a reference to the raw data contained in the dictionary (a pointer).

        Returns:
            [dict]: reference to the raw data contained in the dictionary
        """
        return self._jdata

    @property
    def type(self) -> str:
        """
        Returns the json type, for a dictionary returns 'Object', for list returns 'Array'.

        Returns:
            str: 'Object or 'Array'
        """
        return self._type

    def isArray(self) -> bool:
        """
        Returns "True" if the json is a list of documents (list).
        """
        return self._type == CONST_TYPE_ARRAY

    def isObject(self) -> bool:
        """
        Returns "True" if the json is a single document (dict).
        """
        return self._type == CONST_TYPE_OBJECT

    def getJson(self, flagPretty: bool = False, flagEnsureAscii: bool = False) -> str:
        """
        Convert json to a string.

        Args:
            flagPretty: if "True" the output is a pretty document
            flagEnsureAscii: converts all characters into ASCII, for requests (web APIs) it must be "True".

        Returns:
            str: json document
        """
        if flagPretty:
            return js.dumps(self._jdata, flagPretty=flagPretty, ensure_ascii=flagEnsureAscii)

        return js.dumps(self._jdata, flagPretty=flagPretty, ensure_ascii=flagEnsureAscii)

    def clone(self) -> jDocument:
        """
        Does a deepcopy of the json document.

        Returns:
            jDocument: a copy of the document
        """
        return jDocument(deepcopy(self._jdata))

    def getAttributes(self, flagDeepDocs: bool = True) -> dict:
        """
        Returns a dictionary with the list of attributes contained in the documents, informing the name and type of each attribute.

        Args:
            flagDeepDocs: if "True" then it also returns the attributes of subdocuments that may exist within the main document.

        Returns:
            dict: dictionary with attributes and data types
        """
        # dictionary where the attributes will be cataloged
        attributes = {}

        # if there is a list of objects in the document
        if self._type == CONST_TYPE_ARRAY:
            # checks the attributes of each of them
            if self._jdata:
                list(map(lambda obj: getDocAttributes(attributes, obj, flagDeepDocs=flagDeepDocs), self._jdata))

        # else checks the attributes of the object contained in the document
        else:
            getDocAttributes(attributes, self._jdata, flagDeepDocs=flagDeepDocs)

        return attributes

    def exists(self, attribute: str) -> bool:
        """
        Check if an attribute exists in the document, returns "True" if it exists.

        Examples:
            jPerson.exists('name')

        Returns:
            bool: "True" if attribute exists
        """
        return self.value(attribute, None, flagRaiseError=False)

    def removeAttrib(self, attribute: str | list) -> int:
        """
        For json of type dict removes an attribute from the document, for a list removes the attribute from all elements in the list.

        Examples:
            jPerson.removeAttrib('name')
            jPerson.removeAttrib(['name', 'age'])

        Args:
            attribute (str | list): attribute name or list of names

        Returns:
            int: the number of occurrences removed from the attribute.
        """
        # if a list of attribute names was provided
        q = 0
        if isinstance(attribute, list):
            for at in attribute:
                q += self.removeAttrib(attribute=at)
            return q

        # if the Json document is an object
        if self._type == CONST_TYPE_OBJECT:
            q = 0
            if attribute:
                # if the name of a specific attribute was informed
                if '.' in attribute:
                    # if an attribute of a subObject was informed
                    at = attribute.split('.')
                    lastAt = at.pop()
                    obj = self

                    for atSub in at:
                        obj = obj.get(atSub)

                    q += obj.removeAttrib(lastAt)

                else:
                    # otherwise, it is an attribute of the object itself
                    if attribute in self._jdata:
                        # if attribute exists on this object then remove attribute
                        del self._jdata[attribute]
                        q = 1

            return q

        # otherwise, if it is an ARRAY
        q = 0
        for obj in self:
            # removes the attribute on each object in the list
            q += obj.removeAttrib(attribute=attribute)

        return q

    def value(self, attribute=None, defaultValue: any = None, flagRaiseError: bool = False) -> any:
        """
        Returns the raw data value of an attribute in its native format (whether a "dict" or "list" returns a pointer). If the name of any attribute is not informed, then it returns the object itself.
        The attribute name can be given using the json punctuation convention.

        Examples:
            jSquad.get('team[1].address.street') 	# retorna um 'string'
            jSquad.get('team[1].address') 			# retorna um 'dict'

        Args:
            attribute (str | list): attribute name or list of names
            defaultValue (any) : if the attribute does not exist, return this value
            flagRaiseError (bool) : if True it generates an error if the attribute does not exist, otherwise it returns the defaultValue

        Returns:
            any: if the Json document is an object and the attribute is a string, it returns the value corresponding to the attribute.
            dict: if the Json document is an object and a list of attributes is requested, it returns a dictionary with 'attribute': 'value'.
            list: if the Json document is a list of objects, a list of values.
        """
        # if no attribute was informed, it returns the dictionary or array itself
        if not attribute:
            return self._jdata

        # if a single attribute was entered
        if isinstance(attribute, str):
            # if the Json document is a list
            if self._type == CONST_TYPE_ARRAY:
                # if the sought attribute does not start with "ARRAY"
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
                    else:
                        # coleta uma lista de valores, um para cada objeto da lista
                        return [jDocument(obj).value(attribute, defaultValue) for obj in self._jdata]

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

                    dic = val

            except Exception:
                if flagRaiseError:
                    raise Exception(f"*** {sys.exc_info()[0]}")
                else:
                    val = defaultValue

            return val

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

        return None

    def get(self, attribute: str, defaultValue=None, flagRaiseError: bool = False, flagReturnEmptyListAsDoc: bool = False) -> any:
        """
        Similar to value(), but if the returned value is a 'dict' or 'list' then the returned value is converted to "jDocument".

        Examples:
            jSquad.get('team[1].address.street') 	# retorns a 'string'
            jSquad.get('team[1].address') 			# retorns a 'jDocument'

        Args:
            attribute (str): attribute name.
            defaultValue (any) : if key does not exist, return this value
            flagRaiseError (bool) : if True it generates an error if the attribute does not exist, otherwise it returns the defaultValue
            flagReturnEmptyListAsDoc (bool) : if True, when returning an empty list, convert the list to jDocument

        Returns:
            any: attribute value,.
        """
        val = self.value(attribute, defaultValue, flagRaiseError)

        if isinstance(val, dict):
            return jDocument(val)

        if isinstance(val, list):
            if val and isinstance(val[0], dict):
                return jDocument(val)

            if not val and flagReturnEmptyListAsDoc:
                return jDocument(val)

        return val

    def set(self, values: dict) -> any:
        """
        Adds or updates one or more attributes in the document. If the document is a list then it performs the operation for all documents in the list.

        Examples:
            jPerson = jSquad['team').item(1)
            jPerson.set({'name': 'maria', 'age': 10})

        Args:
            values (dict): dictionary with attributes and values to be updated in the json document.

        Returns:
            any: the value of the last attribute that was added or updated
        """
        # se o documento Json for um objeto
        if self._type == CONST_TYPE_OBJECT:
            if not values.keys():
                return None

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

                    self.get(lstAttrib[0]).set({subAt: values[at]})

                else:
                    if isinstance(values[at], jDocument):
                        # se estivermos atribuindo um documento Json, transforma no valor nativo
                        self._jdata[at] = values[at].value()
                    else:
                        self._jdata[at] = values[at]

            if isinstance(values[returnAt], dict):
                return jDocument(values[returnAt])
            else:
                return values[returnAt]

        else:
            # senão, é uma lista
            # adiciona/atualiza o atributo em todos os objetos da lista
            for obj in self._jdata:
                obj.set(values)

            return None

    def copyFrom(self, jDoc: jDocument):
        """
        Copies to the document all the attributes of another document passed as a parameter.

        Examples:
            jPerson.copyFrom(jOtherPerson)

        Args:
            jDoc: json document from which attributes and values will be copied from.
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

    def getDataType(self, attribute: str) -> str | None:
        """
        Returns the data type of document attribute, the types will be the same as in Python (integer, string, etc.).

        Examples:
            jPerson.getDataType('Address.Street')

        Args:
            attribute: attribute name.

        Returns:
            str: datatype name.
        """
        val = self.value(attribute)

        if not val:
            return None

        if isinstance(val, dict):
            return CONST_TYPE_OBJECT

        if isinstance(val, list):
            return CONST_TYPE_ARRAY

        refind = re.compile(r".*'(.*)'.*")  # RegEx para extrair o tipo do atributo
        gp = refind.search(str(type(val)))
        tp = gp.group(1)

        if '.' in tp:
            tk = tp.split('.')
            tp = tk[-1]

        return tp

    def clear(self):
        """
        Cleans the json content, keeping its type ('Array' or 'Object').
        """
        self._jdata.clear()

    def item(self, position: int) -> any:
        """
        Returns an element from the list of documents, the json needs to be a 'list' otherwise it generates an error.

        Examples:
            # access the fourth element of the list
            jPerson = jTeam.item(3)
            jPerson = jTeam[3]

        Args:
            position (int): position of the element in the list.

        Returns:
            jDocument: item from the list
        """
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        return jDocument(self._jdata[position])

    def addDoc(self, item: jDocument | dict | list) -> jDocument:
        """
        Adds one or more documents to the list, the json needs to be a 'list' otherwise it generates an error.
        When the informed parameter is a list of documents, then this list will be added to the json (at the end of the list).
        Returns the document itself that was included.

        Examples:
            jTeam.addDoc({
                'Name': 'Maria',
                'Age': 30,
                'Address': {
                    'Street': 'Av Pablo Picasso'
                    'Number': 33,
                    'City': 'Sao Paulo'
                }
            })

            jColors.addDoc([
                { 'color': 'red', 'priority': 1},
                { 'color': 'green', 'priority': 2},
            ]

        Args:
            item (jDocument | dict | list): element (list or dict) to be added to the list of documents.

        Returns:
            jDocument: returns the added element
        """
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        if isinstance(item, jDocument):
            obj = item
            if item.type == CONST_TYPE_ARRAY:
                # it is a list
                list(self._jdata).extend(item.value())
            else:
                # it is a dict
                list(self._jdata).append(item.value())
            # endif --

        elif isinstance(item, dict):
            obj = jDocument(item)
            list(self._jdata).append(item)

        elif isinstance(item, list):
            obj = jDocument(item)
            list(self._jdata).extend(item)

        else:
            raise Exception(CONST_ERR_ITEM)

        return obj

    def removeOneDoc(self, filters: dict | list = None) -> int:
        """
        Removes from the list the first N documents that match the informed filter, the json needs to be a 'list' otherwise it generates an error.
        If N is not informed then all documents will be removed.
        The filter is a dictionary with attributes and values, the search is 'Case Insensitive'.
        Returns the number of documents removed from the list.

        Examples:
            jTeam.removeDocs(filters={'Name': 'Maria'})					# removes the elements whose 'Name' is equals to 'Maria'
            jTeam.removeDocs(filters=[{'Name': 'Maria'}, {'Age': 30}])	# removes the elements whose 'Name' is equals to 'Maria' and 'Age' is equals to 30
            jTeam.removeDocs(index=3)									# removes the fourth element from the list

        Args:
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.

        Returns:
            int: the number of documents removed from the list.
        """
        return self.removeDocs(filters=filters, qty=1)

    def removeDocs(self, position: int = None, filters: dict | list = None, qty: int = None) -> int:
        """
        Removes from the list the first document that match the informed filter, the json needs to be a 'list' otherwise it generates an error.
        If N is not informed then all documents will be removed.
        The filter is a dictionary with attributes and values, the search is 'Case Insensitive'.
        Returns the number of documents removed from the list.

        Examples:
            jTeam.removeDocs(filters={'Name': 'Maria'})					# removes the elements whose 'Name' is equals to 'Maria'
            jTeam.removeDocs(filters=[{'Name': 'Maria'}, {'Age': 30}])	# removes the elements whose 'Name' is equals to 'Maria' and 'Age' is equals to 30
            jTeam.removeDocs(index=3)									# removes the fourth element from the list

        Args:
            position: position of the element to be removed.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            qty: maximum amount of documents to be removed, when "None" it will be all.

        Returns:
            int: the number of documents removed from the list, allways 1.
        """
        if position is not None:
            # NÃO foi informado o nome da coleção
            # foi informado um indice
            if self._type != CONST_TYPE_ARRAY:
                raise Exception(CONST_ERR_ARRAY)

            del self._jdata[position]

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
                list(self._jdata).remove(idel)
            # endfor --

            return len(lstRemove)
        # endif --

        if position is None:
            # NÃO foi informado o nome da coleção
            # NÃO foi informado um indice
            if self._type != CONST_TYPE_ARRAY:
                raise Exception(CONST_ERR_ARRAY)

            q = len(self._jdata)
            self._jdata.clear()

            return q
        # endif --

        return 0

    def findDocs(self, filters: dict | list, qty: int = None, flagMacros: bool = False) -> jDocument | None:
        """
        It generates a list with the first N documents that correspond to the informed filter, the json needs to be a 'list' otherwise it generates an error.
        If N is not informed then all documents will be returned.
        The filter is a dictionary with attributes and values, the search is 'Case Insensitive'.
        When "flagMacros" is "True" the routine will test macros provided through the filters. The recognized macros are:
            IN:<value> --> tests whether the attribute's value is contained in <value>.
            NIN:<value> --> tests if attribute value is not contained in <value>.
            CT:<value> --> tests whether <value> is contained in the attribute value.
            NCT:<value> --> tests if <value> is not contained in the attribute value.
            RE:<RegExp> --> tests the attribute value matches the regular expression <RegExp>.

        Examples:
            jPerson = jTeam.findDocs(filters={'name': 'Maria'})
            jPerson = jTeam.findDocs(filters={'name': 'CT:ria'}, flagMacros=True) 	# people whose name contains 'ria'
            jPerson = jTeam.findDocs(filters={'name': 'NCT:ria'}, flagMacros=True) 	# people whose name does not contain 'ria'
            jPerson = jTeam.findDocs(filters={'name': "RE:(g\w+)\W(g\w+)"}, flagMacros=True) 	# people whose name matches the regular expression

        Args:
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            qty: maximum amount of documents to be removed, when "None" it will be all
            flagMacros: if "True" then it searches for macros in the values of the filter rules.

        Returns:
            jDocument: jDocument with Json document containing the list of found documents or None if not found any
        """
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        self._findDocs_flagMacros = flagMacros

        # se foi passado um dicionário de condições monta uma lista com apenas essa condição senão considera esta lista de condições
        self._findDocs_lstFilters = [filters] if isinstance(filters, dict) else filters

        # percorre a lista de objetos
        self._findDocs_qty = qty if qty else -1

        findList = list(filter(self._findDocs_TestDoc, self._jdata))

        self._findDocs_lstFilters = None
        self._findDocs_qty = None

        if not findList:
            return None

        return jDocument(findList)

    def findOneDoc(self, filters: dict | list, flagMacros: bool = False) -> jDocument | None:
        """
        Returns the first document that correspond to the informed filter, the json needs to be a 'list' otherwise it generates an error.
        The filter is a dictionary with attributes and values, the search is 'Case Insensitive'.
        When "flagMacros" is "True" the routine will test macros provided through the filters. The recognized macros are:
            IN:<value> --> tests whether the attribute's value is contained in <value>.
            NIN:<value> --> tests if attribute value is not contained in <value>.
            CT:<value> --> tests whether <value> is contained in the attribute value.
            NCT:<value> --> tests if <value> is not contained in the attribute value.
            RE:<RegExp> --> tests the attribute value matches the regular expression <RegExp>.

        Examples:
            jPerson = jTeam.findOneDoc(filters={'name': 'Maria'})
            jPerson = jTeam.findOneDoc(filters={'name': 'CT:ria'}, flagMacros=True) 	# first person whose name contains 'ria'
            jPerson = jTeam.findOneDoc(filters={'name': 'NCT:ria'}, flagMacros=True) 	# first person whose name does not contain 'ria'
            jPerson = jTeam.findOneDoc(filters={'name': "RE:(g\w+)\W(g\w+)"}, flagMacros=True) 	# first person whose name matches the regular expression

        Args:
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            flagMacros: if "True" then it searches for macros in the values ​​of the filter rules.

        Returns:
            jDocument: json document containing the first documents located or None if not found any
        """
        jObj = self.findDocs(filters, qty=1, flagMacros=flagMacros)

        if jObj:
            return jObj.item(0)

        return None

    def findAnyDocs(self, lstFilters: list, qty: int = None) -> jDocument:
        """
        Searches for text within each document in the list and returns those that match the specified criteria.
        This criterion is made up of a list of values and/or regular expressions.
        The search is 'Case Insensitive' and treats accented characters as non-accented.

        Examples:
            jPeople = jTeam.findOneDoc(filters=["(g\w+)\W(g\w+)"])		# tests if the json document matches regular expression
            jPeople = jTeam.findOneDoc(filters=["Maria", "Paulista"])	# tests whether the two texts are contained in the json document

        Args:
            lstFilters: list of search criteria that can be text or regular expressions.
            qty: maximum number of documents to be searched, when "None" it will be all.

        Returns:
            jDocument: json document containing the list of documents located.
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

            q += 1

            if qty and q > qty:
                break

        return jDocument(findList)

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
        if not self._findDocs_qty:
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
                    if ruleExpr != str(valAttr):
                        return False
                    # endif --

                elif not jDocument._findDocs_TestAttrib(ruleExpr, valAttr):
                    return False
                # endif --
            # endfor --
        # endfor --

        if self._findDocs_qty > 0:
            self._findDocs_qty -= 1
        # endif

        return True

    def findAttribDocs(self, lstAttributes: list, qty: int = None) -> jDocument:
        """
        Returns a list of documents that contain a certain set of attributes, whose names are passed in 'lstAttrib'.

        Examples:
            jPerson = jTeam.findAttribDocs(lstAttrib=['name', 'address.street'])

        Args:
            lstAttributes: list with the names of the attributes to be checked.
            qty: maximum number of documents to be searched, when "None" it will be all.

        Returns:
            jDocument: jDocument with a json document containing the list of documents located.
        """
        filters = {at: '.*' for at in lstAttributes}

        return self.findDocs(filters, qty)

    def sortDocs(self, attribute: str | dict | list) -> jDocument:
        """
        Sort the list of documents, the json needs to be a 'list' otherwise it generates an error.

        Examples:
            jTeam.sortDocs('Name')						# sort by name in ascending order
            jTeam.sortDocs(['Name', 'Address.Street'])	# Sort by name and street in ascending order
            jTeam.sortDocs({'Name', -1})				# sort by name in descending order
            jTeam.sortDocs({'Name', 1})					# sort by name in ascending order

        Args:
            attribute (str|dict|list): attributes to be considered in the ordering.
                                       You can enter a string with the name of the attribute or a list of names;
                                       or you can enter a dictionary with the name of the attribute plus the ordering sequence or a list of dictionaries.

        Return:
            self: json document itself already sorted.
        """
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        if isinstance(attribute, list):
            # se foi informado uma lista de atributos
            list(map(lambda at: self.sortDocs(at), reversed(attribute)))
            return self

        if isinstance(attribute, str):
            # foi informado apenas o nome do atributo
            list(self._jdata).sort(key=lambda e: (not e[attribute], e[attribute]))

        else:
            # foi informado um dicionário com o nome do atributo e a sequência (0 ou 1)
            for sortAttrib, order in attribute.items():
                list(self._jdata).sort(reverse=(True if order != 1 else False), key=lambda e: (not e[sortAttrib], e[sortAttrib]))

        return self

    def searchDocs(self, jOrFilters: jDocument = None, exprFilter: str = None, qty: int = None) -> jDocument:
        """
        Searches the list of documents and returns those that match a set of conditions.
        These conditions can be exposed through a "jDocument" or a Python expression.
        In the case of the Python expression, the attributes of the documents in the list are referenced through the "jDoc" variable.
        Search using Python expression is only recommended for small lists as it is slower by using eval().

        Examples:
            # search for documents with 'Name' equal to 'Maria' and 'Age' greater than 30, or 'Name' equal to 'Marta' and age less than 20
            jOrFilters=[
                {
                    'And': [
                        {'Attribute': 'Name', 'Operator': 'eq', 'Value': 'Maria'},
                        {'Attribute': 'Age', 'Operator': 'gt', 'Value': 30}
                    ]
                },
                {
                    'And': [
                        {'Attribute': 'Name', 'Operator': 'eq', 'Value': 'Marta'},
                        {'Attribute': 'Age', 'Operator': 'lt', 'Value': 20}
                    ]
                }

            ]
            jPerson = jTeam.searchOneDoc(jOrFilters=jOrFilters)

            # alternative with Python expression
            jPerson = jTeam.searchOneDoc(exprFilter="(jDoc['Name'] == 'Maria' and jDoc['Age'] > 30) or (jDoc['Name'] == 'Marta' and jDoc['Age'] < 20)")


        When using jOrFilters, the operator options are:
            eq = equal
            dif = not equal
            lt = less than
            lteq = less than or equal
            gt = greater than
            gteq = greater than or equal
            ct = contain
            nct = not contain
            re = RegExp
            re = RegExp
            in = contained in a list
            nin = NOT contained in a list

            If the field name is "all" then a text search will be performed within the JSON document
            ** In this case, only the "contain" and "not contain" operators are accepted

        Args:
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria
            qty: maximum number of documents to be searched, when "None" it will be all.

        Returns:
            jDocument: list of documents found
        """
        if self._type != CONST_TYPE_ARRAY:
            if self._testDoc(jDocument(self._jdata), jOrFilters):
                return jDocument([jDocument(self._jdata)])
            else:
                return jDocument([])

        self._searhDocs_jOrFilters = jOrFilters
        self._searhDocs_exprFilter = exprFilter
        self._searhDocs_qty = qty if qty else -1

        findList = list(filter(self._searchDocs_TestDoc, self._jdata))

        self._searhDocs_jOrFilters = None
        self._searhDocs_exprFilter = None
        self._searhDocs_qty = None

        return jDocument(findList)

    def searchOneDoc(self, jOrFilters: jDocument = None, exprFilter: str = None) -> jDocument:
        """
        Searches for the firts document that match a set of conditions.
        These conditions can be exposed through a "jDocument" or a Python expression.
        In the case of the Python expression, the attributes of the documents in the list are referenced through the "jDoc" variable.
        Search using Python expression is only recommended for small lists as it is slower by using eval().

        Examples:
            # search for documents with 'Name' equal to 'Maria' and 'Age' greater than 30, or 'Name' equal to 'Marta' and age less than 20
            jOrFilters=[
                {
                    'And': [
                        {'Attribute': 'Name', 'Operator': 'eq', 'Value': 'Maria'},
                        {'Attribute': 'Age', 'Operator': 'gt', 'Value': 30}
                    ]
                },
                {
                    'And': [
                        {'Attribute': 'Name', 'Operator': 'eq', 'Value': 'Marta'},
                        {'Attribute': 'Age', 'Operator': 'lt', 'Value': 20}
                    ]
                }

            ]
            jPerson = jTeam.searchOneDoc(jOrFilters=jOrFilters)

            # alternative with Python expression
            jPerson = jTeam.searchOneDoc(exprFilter="(jDoc['Name'] == 'Maria' and jDoc['Age'] > 30) or (jDoc['Name'] == 'Marta' and jDoc['Age'] < 20)")


        When using jOrFilters, the operator options are:
            eq = equal
            dif = not equal
            lt = less than
            lteq = less than or equal
            gt = greater than
            gteq = greater than or equal
            ct = contain
            nct = not contain
            re = RegExp
            re = RegExp
            in = contained in a list
            nin = NOT contained in a list

            If the field name is "all" then a text search will be performed within the JSON document
            ** In this case, only the "contain" and "not contain" operators are accepted

        Args:
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
            jDocument: document found
        """
        jDoc = self.searchDocs(jOrFilters=jOrFilters, exprFilter=exprFilter, qty=1)
        return jDoc[0] if jDoc else None

    def _searchDocs_TestDoc(self, docdic) -> bool:
        if not self._searhDocs_qty:
            return False
        # endif --

        if self._searhDocs_jOrFilters and self._testDoc(jDocument(docdic), self._searhDocs_jOrFilters):
            return True
        # endif --

        if self._searhDocs_exprFilter:
            # 'jDoc' é uma variável para expressão de validação
            jDoc = jDocument(docdic)
            if eval(self._searhDocs_exprFilter):
                return True
            # endif --
            if jDoc:
                pass
            # endif --
        # endif --

        return False

    def _testDoc(self, jDoc: jDocument, jOrFilters: jDocument) -> bool:
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
                # endif --
            # endfor --

            if flagFind:
                # todas as condições são verdadeiras
                if self._searhDocs_qty > 0:
                    self._searhDocs_qty -= 1
                # endif --
                break
            # endif --
        # endfor --

        return flagFind

    def _getListOfValues(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> list:
        if self._type != CONST_TYPE_ARRAY:
            raise Exception(CONST_ERR_ARRAY)

        if filters:
            jList = self.findDocs(filters)

        elif jOrFilters or exprFilter:
            jList = self.searchDocs(jOrFilters=jOrFilters, exprFilter=exprFilter)

        else:
            jList = jDocument(self._jdata)
        # endif --

        if not jList:
            return []
        # endif --

        lstValues = [jDoc.get(attribute) for jDoc in jList if jDoc[attribute]]

        return lstValues

    def count(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> float | None:
        """
        Returns the number of documents in the list whose 'attrib' attribute is filled.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the number of documents in the list whose 'attrib' attribute is filled.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return len(lstValues)

    def sum(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> float | None:
        """
        Returns the sum of the values of a specific attribute of the documents in the list.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the sum of the values of a specific attribute of the documents in the list.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return sum(lstValues) if lstValues else None

    def min(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> float | None:
        """
        Returns the minimum of the values of a specific attribute of the documents in the list.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the minimum of the values of a specific attribute of the documents in the list.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return min(lstValues) if lstValues else None

    def max(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> float | None:
        """
        Returns the maximum of the values of a specific attribute of the documents in the list.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the maximum of the values of a specific attribute of the documents in the list.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return max(lstValues) if lstValues else None

    def mean(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> float | None:
        """
        Returns the mean of the values of a specific attribute of the documents in the list.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the mean of the values of a specific attribute of the documents in the list.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return statistics.mean(lstValues) if lstValues else None

    def mode(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> float | None:
        """
        Returns the mode of the values of a specific attribute of the documents in the list.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the mode of the values of a specific attribute of the documents in the list.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return statistics.mode(lstValues) if lstValues else None

    def median(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> float | None:
        """
        Returns the median of the values of a specific attribute of the documents in the list.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the median of the values of a specific attribute of the documents in the list.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return statistics.median(lstValues) if lstValues else None

    def median_low(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> float | None:
        """
        Returns the median low of the values of a specific attribute of the documents in the list.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the median low of the values of a specific attribute of the documents in the list.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return statistics.median_low(lstValues) if lstValues else None

    def median_high(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> float | None:
        """
        Returns the median high of the values of a specific attribute of the documents in the list.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the median high of the values of a specific attribute of the documents in the list.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return statistics.median_high(lstValues) if lstValues else None

    def median_grouped(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> float | None:
        """
        Returns the grouped median of the values of a specific attribute of the documents in the list.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the grouped median of the values of a specific attribute of the documents in the list.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return statistics.median_grouped(lstValues) if lstValues else None

    # def quantile(self, attrib: str, quantile: float, method: str = 'linear', lstFilter: list = None, jFilter: jDocument = None, exprFilter: str = None) -> float | None:
    #     lstValues = self._getListOfValues(attrib, lstFilter, jFilter, exprFilter)
    #     return numpy.quantile(lstValues, quantile, method=method) if lstValues else None

    def ocorrences(self, attribute: str, filters: list = None, jOrFilters: jDocument = None, exprFilter: str = None) -> dict | None:
        """
        Returns the number of ocurrencies of the values of a specific attribute of the documents in the list.
        Only documents that match the rules entered in one of the filters will be considered.
        If no filter is specified then all documents will be considered.

        Examples:
            # counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
            jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")

            # counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
            jTeam.count(attrib='Name', filters={"Age": 30})

        Args:
            attribute: name of the attribute to be counted.
            filters: dictionary or dictionary list with attribute and value to filter the documents to be removed from the list.
            jOrFilters: json with the search criteria.
            exprFilter: Python expression with search criteria

        Returns:
             float: the number of ocurrencies of the values of a specific attribute of the documents in the list.
        """
        lstValues = self._getListOfValues(attribute, filters, jOrFilters, exprFilter)
        return dict((item, lstValues.count(item)) for item in set(lstValues)) if lstValues else None


class DotDict(dict):
    def __bool__(self):
        return True

    def __getattr__(self, key):
        try:
            # se o atributo for um dict, retorna o dict
            if isinstance(self[CONST_JDATA][key], dict):
                return DotDict({CONST_JDATA: self[CONST_JDATA][key]})

            # se o atributo for um list, retorna o jDocument
            elif isinstance(self[CONST_JDATA][key], list):
                return jDocument(self[CONST_JDATA][key])

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
                return jDocument(at) if at and isinstance(at[0], dict) else jDotDict({CONST_JDATA: at})

            # senão retorna o elemento
            else:
                return at

        except KeyError as k:
            raise AttributeError(k)

    def __setattr__(self, key, value):
        self[CONST_JDATA][key] = value

    def __delattr__(self, key):
        try:
            del self[CONST_JDATA][key]
        except KeyError as k:
            raise AttributeError(k)
