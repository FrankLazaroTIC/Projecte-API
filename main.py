from fastapi import FastAPI

from db import clientPS

from db import productDB

from model.Product import Product

from schema import producte

from schema import productes

from fastapi import FastAPI, UploadFile, File

app = FastAPI()

##### uvicorn main:app --reload

@app.get("/product/")
def getProduct():
    data = productDB.consulta()
    datajson = productes.products_schema(data)
    return datajson

#####

@app.get("/product/{id}")
def getProductById(id:int):
    data = productDB.consultaById(id)
    datajson = producte.product_schema(data)
    return datajson

#####

@app.post("/product/")
def createProduct(prod: Product):
    productDB.crear(prod)
    return {"message":f"Producto {prod.name} creado correctamente"}

#####

@app.put("/product/")
def updateProduct(prod:Product):
    productDB.actualitzar(prod)
    return { f"Producte {prod.name} actualitzat"}

@app.put("/product/{id}")
def updateProductById(id:int, prod:Product):
    productDB.actualitzarById(prod)
    return {f"Producte {prod.name} amb el id {id} actualitzat"}

#####

@app.delete("/product/{id}")
def deleteProductById(id:int,prod:Product):
    productDB.borrar(id)
    return {f"Producte {prod.name} borrat!"}

#####

@app.get("/productAll/")
def allProducts():
    json_data = productDB.productAll()
    return json_data

@app.post("/loadProducts")
async def create_upload_file(file: UploadFile):
    csvProducts= productDB.pujarCSV(file)
    return csvProducts