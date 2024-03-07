from fastapi import FastAPI

from db import clientPS

from db import productDB

from model.Product import Product

from schema import producte

from schema import productes

from fastapi import FastAPI, UploadFile, File

app = FastAPI()

##### uvicorn main:app --reload

#### Retorna un producte
@app.get("/product/")
def getProduct():
    data = productDB.consulta()
    datajson = productes.products_schema(data)
    return datajson

##### Retorna un producte per ID
@app.get("/product/{id}")
def getProductById(id:int):
    data = productDB.consultaById(id)
    datajson = producte.product_schema(data)
    return datajson

##### Crea un producte
@app.post("/product/")
def createProduct(prod: Product):
    productDB.crear(prod)
    return {"message":f"Producto {prod.name} creado correctamente"}

##### Actualitzem un producte
@app.put("/product/")
def updateProduct(prod:Product):
    productDB.actualitzar(prod)
    return { f"Producte {prod.name} actualitzat"}

#### Actualitzem un producte per ID
@app.put("/product/{id}")
def updateProductById(id:int, prod:Product):
    productDB.actualitzarById(prod)
    return {f"Producte {prod.name} amb el id {id} actualitzat"}

##### Borrem un producte
@app.delete("/product/{id}")
def deleteProductById(id:int,prod:Product):
    productDB.borrar(id)
    return {f"Producte {prod.name} borrat!"}

##### Retorna tots els productes
@app.get("/productAll/")
def allProducts():
    json_data = productDB.productAll()
    return json_data

#### Podem pujar productes a la bd desde un CSV
@app.post("/loadProducts")
async def create_upload_file(file: UploadFile):
    csvProducts= productDB.pujarCSV(file)
    return csvProducts