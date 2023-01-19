import json
from jDocument import jDocument


def loadJsonSample(filename: str) -> dict | list:
    with open(filename) as f:
        filedata = json.load(f)
    return filedata


# products sample
print("\n" + '-' * 20 + " STATISTICS")
data = loadJsonSample('../tests/products_sample.json')
jProducts = jDocument(data)

print(f"doc type = '{jProducts.type}'")
print(f"IsArray = {jProducts.isArray()}")
print(f"Num of itens = {len(jProducts)}")
print(f"Item number 3 = {jProducts[3]}")
print(f"Num of itens of type 'fruit' = {jProducts.count(attribute='title', filters=[{'type': 'fruit'}])}")
print(f"Num of itens per type = {jProducts.occurrences(attribute='type')}")
print(f"Max price = {jProducts.max('features.price')}")
print(f"Mean price = {jProducts.mean('features.price')}")

jOrFilters = jDocument([
    {
        'And': [
            {
                'Attribute': 'features.price',
                'Operator': 'gt',
                'Value': 28
            }
        ]
    }
])

print(f"Search products whose price is greater then 28 with a filter = {jProducts.searchDocs(jOrFilters=jOrFilters)}")
print("Search products whose price is greater then 28 with a expression = {0}".format(jProducts.searchDocs(exprFilter='jDoc["features.price"] > 28')))

jFirstProduct = jProducts.searchOneDoc(jOrFilters=jOrFilters)

print(f"(jDocment) First product whose price is greater than 28 = {jFirstProduct}")
print(f"(dict)     First product whose price is greater than 28 = {jFirstProduct.value()}")
print(f"(string)   First product whose price is greater than 28 = {jFirstProduct.getJson(flagPretty=False)}")

print(f"Price of first product whose price is greater than 28 = {jFirstProduct['features.price']}")

# page sample
print("\n" + '-' * 20 + " BRACKET NOTATION")
data = loadJsonSample('../tests/page_sample.json')
jPage = jDocument(data)

print(f"doc type = '{jPage.type}'")
print(f"IsObject = {jPage.isObject()}")
print(f"total_pages = {jPage['totals.pages']}")
print(f"data of name equal to 'fuchsia rose' = {jPage['data[name=fuchsia rose]']}")
print(f"data of year equal to '2000' = {jPage['data[year=2000]']}")
print(f"data name of year equal to '2000' = {jPage['data[year=2000].name']}")
print(f"list of attributes = {jPage.getAttributes()}")
print(f"datatype of attribute 'total' = {jPage.getDataType('total')}")

# page sample
print("\n" + '-' * 20 + " DOT NOTATION")
data = loadJsonSample('../tests/page_sample.json')
jPage = jDocument(data)

print(f"doc type = '{jPage.type}'")
print(f"IsObject = {jPage.isObject()}")
print(f"total_pages = {jPage.doc.totals.pages}")
print(f"data of name equal to 'fuchsia rose' = { [item for item in jPage.doc.data if item.doc.name == 'fuchsia rose'] }")
print(f"data of year equal to '2000' = { [item for item in jPage.doc.data if item.doc.year == 2000] }")
print(f"data name of year equal to '2000' = { [item.doc.name for item in jPage.doc.data if item.doc.year == 2000] }")
print(f"list of attributes = {jPage.getAttributes()}")
print(f"datatype of attribute 'total' = {jPage.getDataType('total')}")
