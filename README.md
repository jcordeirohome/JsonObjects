=================
jDocument
=================

2022/january/1 - Jose Cordeiro

The jDocument class allows you to encapsulate a json document (dict or a list) and perform a lot of operations to read, update and add data.

    #create a jDocument from a json file
    filedata = open('team.json').readlines()
    jTeam = jDocument(filedata)
    
    #create a jDocument from a dictionary
    jTeam = jDocument({"Name": "Maria","Age": 30})
    
    #create a jDocument from a list
    jTeam = jDocument([

        {

            "Name": "Maria",
            "Age": 30

        },
        {

            "Name": "Jose",
            "Age": 50

        }

    ])

- **getAttributes()**: Returns a dictionary with the list of attributes contained in the documents, informing the name and type of each attribute.
- **exists()**: Check if an attribute exists in the document, returns "True" if it exists.
- **removeAttrib()**: For json of type dict removes an attribute from the document, for a list removes the attribute from all elements in the list.
- **value()**: Returns the raw data value of an attribute in its native format (whether a "dict" or "list" returns a pointer). The attribute name can be given using the json dot convention.
- **get()**: Returns the data value of an attribute. Tf the returned value is a 'dict' or 'list' then the returned value is converted to "jDocument". The attribute name can be given using the json dot convention.
- **set()**: Adds or updates one or more attributes in the document. If the document is a list then it performs the operation for all documents in the list.
- **copyFrom()**: Copies to the document all the attributes of another document passed as a parameter.
- **getDataType()**: Returns the data type of document attribute, the types will be the same as in Python (integer, string, etc.).
- **clear()**: Cleans the json content, keeping its type ('Array' or 'Object').
- **item()**: Returns an element from the list of documents, the json needs to be a 'list' otherwise it generates an error.


    #access the fourth element of the list
    jPerson = jTeam.item(3)
    jPerson = jTeam[3]


- **addDoc()**: Adds one or more documents to the list, the json needs to be a 'list' otherwise it generates an error. When the informed parameter is a list of documents, then this list will be added to the json (at the end of the list). Returns the document itself that was included.


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

- **removeOneDoc()**: Removes from the list the first N documents that match the informed filter, the json needs to be a 'list' otherwise it generates an error. If N is not informed then all documents will be removed. The filter is a dictionary with attributes and values, the search is 'Case Insensitive'. Returns the number of documents removed from the list.

 
    #removes the elements whose 'Name' is equals to 'Maria'
    jTeam.removeDocs(filters={'Name': 'Maria'})
    
    #removes the elements whose 'Name' is equals to 'Maria' and 'Age' is equals to 30
    jTeam.removeDocs(filters=[{'Name': 'Maria'}, {'Age': 30}])
    
    #removes the fourth element from the list
    jTeam.removeDocs(index=3)

- **removeDocs()**:  Removes from the list the first document that match the informed filter, the json needs to be a 'list' otherwise it generates an error. If N is not informed then all documents will be removed. The filter is a dictionary with attributes and values, the search is 'Case Insensitive'. Returns the number of documents removed from the list.


    #removes the elements whose 'Name' is equals to 'Maria'
    jTeam.removeDocs(filters={'Name': 'Maria'})
    
    #removes the elements whose 'Name' is equals to 'Maria' and 'Age' is equals to 30
    jTeam.removeDocs(filters=[{'Name': 'Maria'}, {'Age': 30}])
    
    #removes the fourth element from the list
    jTeam.removeDocs(index=3)

- **findDocs()**: It generates a list with the first N documents that correspond to the informed filter, the json needs to be a 'list' otherwise it generates an error.  If N is not informed then all documents will be returned.  The filter is a dictionary with attributes and values, the search is 'Case Insensitive'.


    jPerson = jTeam.findDocs(filters={'name': 'Maria'})
    
    #people whose name contains 'ria'
    jPerson = jTeam.findDocs(filters={'name': 'CT:ria'}, flagMacros=True)
    
    #people whose name does not contain 'ria'
    jPerson = jTeam.findDocs(filters={'name': 'NCT:ria'}, flagMacros=True)
    
    #people whose name matches the regular expression
    jPerson = jTeam.findDocs(filters={'name': "RE:(g\w+)\W(g\w+)"}, flagMacros=True)

- **findOneDoc()**: Returns the first document that correspond to the informed filter, the json needs to be a 'list' otherwise it generates an error. The filter is a dictionary with attributes and values, the search is 'Case Insensitive'. When "flagMacros" is "True" the routine will test macros provided through the filters.


    jPerson = jTeam.findOneDoc(filters={'name': 'Maria'})
    
    #first person whose name contains 'ria'
    jPerson = jTeam.findOneDoc(filters={'name': 'CT:ria'}, flagMacros=True)
    
    #first person whose name does not contain 'ria'
    jPerson = jTeam.findOneDoc(filters={'name': 'NCT:ria'}, flagMacros=True)
    
    #first person whose name matches the regular expression
    jPerson = jTeam.findOneDoc(filters={'name': "RE:(g\w+)\W(g\w+)"}, flagMacros=True)

    
- **findAnyDocs()**: Searches for text within each document in the list and returns those that match the specified criteria.  This criterion is made up of a list of values and/or regular expressions.  The search is 'Case Insensitive' and treats accented characters as non-accented.


    #tests if the json document matches regular expression
    jPeople = jTeam.findOneDoc(filters=["(g\w+)\W(g\w+)"])
    
    #tests whether the two texts are contained in the json document
    jPeople = jTeam.findOneDoc(filters=["Maria", "Paulista"])

- **findAttribDocs()**: Returns a list of documents that contain a certain set of attributes, whose names are passed in 'lstAttrib'.


    jPerson = jTeam.findAttribDocs(lstAttrib=['name', 'address.street'])

- **sortDocs()**: Sort the list of documents, the json needs to be a 'list' otherwise it generates an error.
 

    #sort by name in ascending order
    jTeam.sortDocs('Name')
    
    #Sort by name and street in ascending order
    jTeam.sortDocs(['Name', 'Address.Street'])
    
    #sort by name in descending order
    jTeam.sortDocs({'Name', -1})
    
    #sort by name in ascending order
    jTeam.sortDocs({'Name', 1})

- **searchDocs()**: Searches the list of documents and returns those that match a set of conditions.  These conditions can be exposed through a "jDocument" or a Python expression. In the case of the Python expression, the attributes of the documents in the list are referenced through the "jDoc" variable. Search using Python expression is only recommended for small lists as it is slower by using eval().


    #search for documents with 'Name' equal to 'Maria' and 'Age' greater than 30, or 'Name' equal to 'Marta' and age less than 20
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
    
    #alternative with Python expression
    jPerson = jTeam.searchOneDoc(exprFilter="(jDoc['Name'] == 'Maria' and jDoc['Age'] > 30) or (jDoc['Name'] == 'Marta' and jDoc['Age'] < 20)")


- **searchOneDoc()**: Searches for the first document that match a set of conditions. These conditions can be exposed through a "jDocument" or a Python expression. In the case of the Python expression, the attributes of the documents in the list are referenced through the "jDoc" variable. Search using Python expression is only recommended for small lists as it is slower by using eval().
 

    #search for documents with 'Name' equal to 'Maria' and 'Age' greater than 30, or 'Name' equal to 'Marta' and age less than 20
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

- **count()**: Returns the number of documents in the list whose 'attrib' attribute is filled. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered.


    #counts how many documents have the 'Name' attribute filled in and with 'Age' > 30
    res = jTeam.count(attrib='Name', exprFilter="jDoc['Age'] > 30")
    
    #counts how many documents have the attribute 'Name' filled in and with 'Age' = 30
    res = jTeam.count(attrib='Name', filters={"Age": 30})

- **sum()**: Returns the sum of the values of a specific attribute of the documents in the list. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered.


    #calculate de sum value of 'Age' for 'Age' > 30
    res = jTeam.sum(attrib='Age', exprFilter="jDoc['Age'] > 30")
    
    #calculate de sum value of 'Age' for 'Name' equals to 'Maria'
    res = jTeam.sum(attrib='Name', filters={"Name": 'Maria'})

- **min()**: Returns the minimum of the values of a specific attribute of the documents in the list. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered.
 

    #calculate de min value of 'Age' for 'Age' > 30
    res = jTeam.max(attrib='Age', exprFilter="jDoc['Age'] > 30")
    
    #calculate de min value of 'Age' for 'Name' equals to 'Maria'
    res = jTeam.max(attrib='Name', filters={"Name": 'Maria'})

- **max()**: Returns the maximum of the values of a specific attribute of the documents in the list. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered.


    #calculate de max value of 'Age' for 'Age' > 30
    res = jTeam.max(attrib='Age', exprFilter="jDoc['Age'] > 30")
    
    #calculate de max value of 'Age' for 'Name' equals to 'Maria'
    res = jTeam.max(attrib='Name', filters={"Name": 'Maria'})

- **mean()**: Returns the mean of the values of a specific attribute of the documents in the list. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered.


    #calculate de the mean form then documents having the 'Name' attribute filled in and with 'Age' > 30
    res = jTeam.mean(attrib='Name', exprFilter="jDoc['Age'] > 30")
    
    #calculate de the mean form then documents having attribute 'Name' filled in and with 'Age' = 30
    res = jTeam.mean(attrib='Name', filters={"Age": 30})

- **mode()**: Returns the mode of the values of a specific attribute of the documents in the list. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered.


    #calculate the mode of the values of 'Name' with 'Age' > 18
    res = jTeam.mode(attrib='Name', exprFilter="jDoc['Age'] > 18")
    
    #calculate the mode of the values of 'Name' with 'Age' = 18
    res = jTeam.modet(attrib='Name', filters={"Age": 18})

- **median()**: Returns the median of the values of a specific attribute of the documents in the list. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered. .. code-block:: Python


    #calculate the mode of the values of 'Name' with 'Age' > 18
    res = jTeam.median(attrib='Name', exprFilter="jDoc['Age'] > 30")
    
    #calculate the mode of the values of 'Name' with 'Age' = 18
    res = jTeam.median(attrib='Name', filters={"Age": 18})

- **median_low()**: Returns the median low of the values of a specific attribute of the documents in the list. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered.

 
    #calculate the mode of the values of 'Name' with 'Age' > 18
    res = jTeam.median_low(attrib='Name', exprFilter="jDoc['Age'] > 30")
    
    #calculate the mode of the values of 'Name' with 'Age' = 18
    res = jTeam.median_low(attrib='Name', filters={"Age": 18})

- **median_high()**: Returns the median high of the values of a specific attribute of the documents in the list. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered.

 
    #calculate the mode of the values of 'Name' with 'Age' > 18
    res = jTeam.median_high(attrib='Name', exprFilter="jDoc['Age'] > 30")
    
    #calculate the mode of the values of 'Name' with 'Age' = 18
    res = jTeam.median_high(attrib='Name', filters={"Age": 18})

- **median_grouped()**: Returns the grouped median of the values of a specific attribute of the documents in the list. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered.
 

    #calculate the mode of the values of 'Name' with 'Age' > 18
    res = jTeam.median_grouped(attrib='Name', exprFilter="jDoc['Age'] > 30")
    
    #calculate the mode of the values of 'Name' with 'Age' = 18
    res = jTeam.median_grouped(attrib='Name', filters={"Age": 18})

- **occurrences()**: Returns the number of occurrences of the values of a specific attribute of the documents in the list. Only documents that match the rules entered in one of the filters will be considered. If no filter is specified then all documents will be considered.


    #calculate the number of occurrences for each 'Name' with 'Age' > 18
    res = jTeam.occurrences(attrib='Name', exprFilter="jDoc['Age'] > 18")
    
    #calculate the number of occurrences for each 'Name' with 'Age' = 18
    res = jTeam.occurrences(attrib='Name', filters={"Age": 18)
