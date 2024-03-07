from db import clientPS

from model.Product import Product as Product

import json

from typing import List

import csv

from fastapi import UploadFile

from datetime import datetime

import pandas as pd

#def producteAll():
    #[for]

#def producte(product):
#    json= f"'id':{product.id}, 'nom':{product.name}"
#    return "{"+json+"}"

def consulta():   
    try:
        conn = clientPS.client()
        
        cur = conn.cursor()
        
        cur.execute(f"SELECT * FROM PRODUCT")
        
        data= cur.fetchall()
        
    except Exception as e:
        print(f"ERRORRRRR: {e}")
        
    finally:    
        conn.close()
    
    return data

def consultaById(id:int):
    try:
        conn = clientPS.client()
        
        cur = conn.cursor()
        
        cur.execute(f"SELECT * FROM PRODUCT WHERE product_id = {id}")
        
        data= cur.fetchone()
        
    except Exception as e:
        print(f"ERRORRRRR: {e}")
        
    finally:    
        conn.close()
    
    return data

def crear(prod: Product):
    try:
        conn = clientPS.client()
        cur = conn.cursor()
        
        sql = f"""
            INSERT INTO PRODUCT (product_id, name, description, company, price, units, subcategory_id, created_at, updated_at)
            VALUES ({prod.product_id}, '{prod.name}', '{prod.description}', '{prod.company}', {prod.price}, {prod.units}, {prod.subcategory_id}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP )
            """
        
        cur.execute(sql)
        
        conn.commit()
        
        message = "Producto insertado correctamente."
    except Exception as e:
        message = f"Error al insertar el producto: {e}"
    finally:
        conn.close()
    
    return message

def actualitzar(prod: Product):
    try:
        conn = clientPS.client()
        
        cur = conn.cursor()
        
        sql = f"""
            UPDATE PRODUCT 
            SET name = '{prod.name}',
                description = '{prod.description}',
                company = '{prod.company}',
                price = {prod.price},
                units = {prod.units},
                subcategory_id = {prod.subcategory_id},
                updated_at = CURRENT_TIMESTAMP
            WHERE product_id = {prod.product_id};
        """
        cur.execute(sql)
        
        conn.commit()
        
    except Exception as e:
        print(f"ERRORRRRR: {e}")
        
    finally:    
        conn.close()
    
    return f"Producte {prod.name} actualitzat"

def actualitzarById(id,prod: Product):
    try:
        conn = clientPS.client()
        
        cur = conn.cursor()
        
        sql = f"""
            UPDATE PRODUCT 
            SET name = '{prod.name}',
                description = '{prod.description}',
                company = '{prod.company}',
                price = {prod.price},
                units = {prod.units},
                subcategory_id = {prod.subcategory_id},
                updated_at = CURRENT_TIMESTAMP
            WHERE product_id = {id};
            """
        
        cur.execute(sql)
        
        conn.commit()
        
    except Exception as e:
        print(f"ERRORRRRR: {e}")
        
    finally:    
        conn.close()
    
    return f"Producte {prod.name} amb {id} actualitzat"

def borrar(id):
    try:
        conn = clientPS.client()
        
        cur = conn.cursor()
        
        sql = f"""
            DELETE FROM PRODUCT WHERE product_id = {id};
            """
        
        cur.execute(sql)
        
        conn.commit()
        
    except Exception as e:
        print(f"ERRORRRRR: {e}")
        
    finally:    
        conn.close()
    
    return f"Producte amb {id} eliminat"
    
def productAll():
    try:
        conn = clientPS.client()
        cur = conn.cursor()
        cur.execute("""
            SELECT c.name AS category_name, 
                   sc.name AS subcategory_name, 
                   p.name AS product_name, 
                   p.company AS product_brand, 
                   p.price
            FROM Product p
            INNER JOIN Subcategory sc ON p.subcategory_id = sc.subcategory_id
            INNER JOIN Category c ON sc.category_id = c.category_id
            """)
        data = cur.fetchall()
    except Exception as e:
        print(f"ERROR: {e}")
        return None
    finally:
        conn.close()
    
    # Formatear los datos en una lista de diccionarios
    products_list = []
    for row in data:
        product_dict = {
            'Nom_Categoria': row[0],
            'Nom_Subcategoria': row[1],
            'Nom_Producte': row[2],
            'Marca_Producte': row[3],
            'Preu': float(row[4])
        }
        products_list.append(product_dict)
    
    # Convertir la lista de diccionarios a JSON
    json_data = json.dumps(products_list)
    
    return json_data

def impCategoria(category_id, name):
    try: 
        conn = clientPS.client()
        
        cur = conn.cursor()
        
        tiempo_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        sql = f"""SELECT * FROM category WHERE category_id={category_id};"""
        cur.execute(sql)
        
        if(cur.fetchone() is not None):
            cur.execute(f"""UPDATE category
                        SET category_id={category_id}, name='{name}', updated_at='{tiempo_actual}'
	                    WHERE category_id={category_id};""")
        else:
            cur.execute(f"INSERT INTO category (category_id, name, created_at, updated_at) VALUES ({category_id}, '{name}', '{tiempo_actual}', '{tiempo_actual}')")
        
        conn.commit()
        
    except Exception as e:
        print(f'Error conexió {e}')
    finally:
        conn.close()

def impSubcategory(subcategory_id, name, category_id):
    try:
        conn = clientPS.client()
        
        cur = conn.cursor()
        
        tiempo_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        sql= f"""SELECT * FROM subcategory WHERE subcategory_id={subcategory_id};"""
        
        cur.execute(sql)
        
        if(cur.fetchone() is not None):
            cur.execute(f"""UPDATE subcategory
                        SET subcategory_id={subcategory_id}, name='{name}', category_id={category_id}, updated_at='{tiempo_actual}'
	                    WHERE category_id={category_id};""")
        else:
            cur.execute(f"INSERT INTO subcategory (subcategory_id, name, category_id, created_at, updated_at) VALUES ({subcategory_id}, '{name}', {category_id}, '{tiempo_actual}', '{tiempo_actual}')")
        conn.commit()
        
    except Exception as e:
        print(f'Erroe conexió {e}')
    finally:
        conn.close()

def crearProducteCSV(product_id, name, description, company, price, units, subcategory_id):
    try:
        conn = clientPS.client()
        
        cur = conn.cursor()
        
        tiempo_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        sql= f"""SELECT * FROM product WHERE product_id={product_id};"""
        
        cur.execute(sql)
        
        if(cur.fetchone() is not None):
            cur.execute(f"""UPDATE product
                        SET product_id={product_id}, name='{name}', description='{description}', company='{company}', price={price}, units={units} , subcategory_id={subcategory_id}, updated_at='{tiempo_actual}'
	                    WHERE product_id={product_id};""")
        else:
            cur.execute(f"INSERT INTO product (product_id, name, description, company, price, units, subcategory_id, created_at, updated_at) VALUES ({product_id}, '{name}', '{description}', '{company}', {price}, {units}, {subcategory_id}, '{tiempo_actual}', '{tiempo_actual}')")
        
        conn.commit()
        
    except Exception as e:
        print(f'Erroer conexió {e}')
    finally:
        conn.close()
        
def pujarCSV(fitcherCSV): 
    try:
        conn = clientPS.client()
        
        df = pd.read_csv(fitcherCSV.file, header=0 )
        
        for index, row in df.iterrows():
            fila= row.to_dict()
            
            impCategoria(fila["id_categoria"], fila["nom_categoria"])
            impSubcategory(fila["id_subcategoria"], fila["nom_subcategoria"], fila["id_categoria"])
            crearProducteCSV(fila["id_producto"], fila["nom_producto"], fila["descripcion_producto"], fila["companyia"], fila["precio"], fila["unidades"], fila["id_subcategoria"])

        conn.commit()
        return "Carrega de dades massives DONE!!!!"
    except Exception as e:
        return ""
    finally:
        conn.close() 