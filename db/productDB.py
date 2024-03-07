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

# Definició de la funció per consultar tots els productes de la base de dades.
def consulta():   
    try:
        # Establiment de connexió amb la base de dades.
        conn = clientPS.client()
        
        # Creació d'un objecte cursor per executar consultes SQL.
        cur = conn.cursor()
        
        # Execució de la consulta SQL per seleccionar tots els productes.
        cur.execute(f"SELECT * FROM PRODUCT")
        
        # Recollida de totes les dades resultants de la consulta.
        data = cur.fetchall()
        
    except Exception as e:
        # Captura d'errors i impressió del missatge d'error.
        print(f"ERRORRRRR: {e}")
        
    finally:
        # Tancament de la connexió a la base de dades.
        conn.close()
    
    # Retorn del conjunt de dades obtingut de la consulta.
    return data

# Definició de la funció per consultar un producte segons el seu identificador.
def consultaById(id:int):
    try:
        # Establiment de connexió amb la base de dades.
        conn = clientPS.client()
        
        # Creació d'un objecte cursor per executar consultes SQL.
        cur = conn.cursor()
        
        # Execució de la consulta SQL per seleccionar un producte específic segons l'ID proporcionat.
        cur.execute(f"SELECT * FROM PRODUCT WHERE product_id = {id}")
        
        # Recollida d'una sola fila resultants de la consulta.
        data = cur.fetchone()
        
    except Exception as e:
        # Captura d'errors i impressió del missatge d'error.
        print(f"ERRORRRRR: {e}")
        
    finally:
        # Tancament de la connexió a la base de dades.
        conn.close()
    
    # Retorn de les dades del producte seleccionat.
    return data

# Definició de la funció per crear un nou producte a la base de dades.
def crear(prod: Product):
    try:
        # Establiment de connexió amb la base de dades.
        conn = clientPS.client()
        cur = conn.cursor()
        
        # Creació de la sentència SQL per inserir un nou producte amb les dades proporcionades.
        sql = f"""
            INSERT INTO PRODUCT (product_id, name, description, company, price, units, subcategory_id, created_at, updated_at)
            VALUES ({prod.product_id}, '{prod.name}', '{prod.description}', '{prod.company}', {prod.price}, {prod.units}, {prod.subcategory_id}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP )
            """
        
        # Execució de la sentència SQL.
        cur.execute(sql)
        
        # Confirmació dels canvis fets a la base de dades.
        conn.commit()
        
        # Missatge de confirmació de la inserció del producte.
        message = "Producto insertado correctamente."
    except Exception as e:
        # Missatge d'error en cas de fallo en la inserció del producte.
        message = f"Error al insertar el producto: {e}"
    finally:
        # Tancament de la connexió a la base de dades.
        conn.close()
    
    # Retorn del missatge sobre l'estat de la inserció del producte.
    return message

# Definició de la funció per actualitzar un producte a la base de dades.
def actualitzar(prod: Product):
    try:
        # Establiment de connexió amb la base de dades.
        conn = clientPS.client()
        
        # Creació d'un objecte cursor per executar consultes SQL.
        cur = conn.cursor()
        
        # Creació de la sentència SQL per actualitzar les dades del producte.
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
        # Execució de la sentència SQL.
        cur.execute(sql)
        
        # Confirmació dels canvis fets a la base de dades.
        conn.commit()
        
    except Exception as e:
        # Captura d'errors i impressió del missatge d'error.
        print(f"ERRORRRRR: {e}")
        
    finally:
        # Tancament de la connexió a la base de dades.
        conn.close()
    
    # Retorn del missatge de confirmació de l'actualització del producte.
    return f"Producte {prod.name} actualitzat"

# Definició de la funció per actualitzar un producte a la base de dades segons el seu ID.
def actualitzarById(id,prod: Product):
    try:
        # Establiment de connexió amb la base de dades.
        conn = clientPS.client()
        
        # Creació d'un objecte cursor per executar consultes SQL.
        cur = conn.cursor()
        
        # Creació de la sentència SQL per actualitzar les dades del producte segons l'ID proporcionat.
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
        
        # Execució de la sentència SQL.
        cur.execute(sql)
        
        # Confirmació dels canvis fets a la base de dades.
        conn.commit()
        
    except Exception as e:
        # Captura d'errors i impressió del missatge d'error.
        print(f"ERRORRRRR: {e}")
        
    finally:
        # Tancament de la connexió a la base de dades.
        conn.close()
    
    # Retorn del missatge de confirmació de l'actualització del producte amb l'ID corresponent.
    return f"Producte {prod.name} amb {id} actualitzat"

# Definició de la funció per eliminar un producte de la base de dades segons el seu ID.
def borrar(id):
    try:
        # Establiment de connexió amb la base de dades.
        conn = clientPS.client()
        
        # Creació d'un objecte cursor per executar consultes SQL.
        cur = conn.cursor()
        
        # Creació de la sentència SQL per eliminar el producte amb l'ID proporcionat.
        sql = f"""
            DELETE FROM PRODUCT WHERE product_id = {id};
            """
        
        # Execució de la sentència SQL.
        cur.execute(sql)
        
        # Confirmació dels canvis fets a la base de dades.
        conn.commit()
        
    except Exception as e:
        # Captura d'errors i impressió del missatge d'error.
        print(f"ERRORRRRR: {e}")
        
    finally:
        # Tancament de la connexió a la base de dades.
        conn.close()
    
    # Retorn del missatge de confirmació de l'eliminació del producte amb l'ID corresponent.
    return f"Producte amb {id} eliminat"

# Funció per recuperar tots els productes de la base de dades.
def productAll():
    try:
        # Establiment de connexió amb la base de dades.
        conn = clientPS.client()
        
        # Creació d'un objecte cursor per executar consultes SQL.
        cur = conn.cursor()
        
        # Execució de la consulta SQL per recuperar tots els productes amb informació rellevant.
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
        
        # Obtenció de les dades recuperades de la consulta.
        data = cur.fetchall()
    
    except Exception as e:
        # Captura d'errors i impressió del missatge d'error.
        print(f"ERROR: {e}")
        return None
    
    finally:
        # Tancament de la connexió a la base de dades.
        conn.close()

    # Creació de la llista de productes i formatatge de les dades en un diccionari.
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
    
    # Conversió de la llista de diccionaris a JSON.
    json_data = json.dumps(products_list)
    
    # Retorn del JSON amb les dades dels productes.
    return json_data

# Funció per importar una categoria a la base de dades.
def impCategoria(category_id, name):
    try: 
        # Establiment de connexió amb la base de dades.
        conn = clientPS.client()
        
        # Creació d'un objecte cursor per executar consultes SQL.
        cur = conn.cursor()
        
        # Obtenció de l'hora actual per a la data.
        tiempo_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Consulta SQL per comprovar si la categoria ja existeix a la base de dades.
        sql = f"""SELECT * FROM category WHERE category_id={category_id};"""
        cur.execute(sql)
        
        # Comprovació si la categoria ja existeix i actualització o inserció de la mateixa.
        if(cur.fetchone() is not None):
            cur.execute(f"""UPDATE category
                        SET category_id={category_id}, name='{name}', updated_at='{tiempo_actual}'
	                    WHERE category_id={category_id};""")
        else:
            cur.execute(f"INSERT INTO category (category_id, name, created_at, updated_at) VALUES ({category_id}, '{name}', '{tiempo_actual}', '{tiempo_actual}')")
        
        # Confirmació dels canvis fets a la base de dades.
        conn.commit()
        
    except Exception as e:
        # Captura d'errors i impressió del missatge d'error.
        print(f'Error conexió {e}')
        
    finally:
        # Tancament de la connexió a la base de dades.
        conn.close()

# Funció per importar una subcategoria a la base de dades.
def impSubcategory(subcategory_id, name, category_id):
    try:
        # Establiment de connexió amb la base de dades.
        conn = clientPS.client()
        
        # Creació d'un objecte cursor per executar consultes SQL.
        cur = conn.cursor()
        
        # Obtenció de l'hora actual per a la data.
        tiempo_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Consulta SQL per comprovar si la subcategoria ja existeix a la base de dades.
        sql= f"""SELECT * FROM subcategory WHERE subcategory_id={subcategory_id};"""
        
        cur.execute(sql)
        
        # Comprovació si la subcategoria ja existeix i actualització o inserció de la mateixa.
        if(cur.fetchone() is not None):
            cur.execute(f"""UPDATE subcategory
                        SET subcategory_id={subcategory_id}, name='{name}', category_id={category_id}, updated_at='{tiempo_actual}'
	                    WHERE category_id={category_id};""")
        else:
            cur.execute(f"INSERT INTO subcategory (subcategory_id, name, category_id, created_at, updated_at) VALUES ({subcategory_id}, '{name}', {category_id}, '{tiempo_actual}', '{tiempo_actual}')")
        
        # Confirmació dels canvis fets a la base de dades.
        conn.commit()
        
    except Exception as e:
        # Captura d'errors i impressió del missatge d'error.
        print(f'Erroe conexió {e}')
        
    finally:
        # Tancament de la connexió a la base de dades.
        conn

# Función para crear o actualizar un producto desde un archivo CSV.
def crearProducteCSV(product_id, name, description, company, price, units, subcategory_id):
    try:
        # Establecer conexión con la base de datos.
        conn = clientPS.client()
        
        # Crear un cursor para ejecutar consultas SQL.
        cur = conn.cursor()
        
        # Obtener la fecha y hora actual en el formato especificado.
        tiempo_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Consultar si el producto ya existe en la base de datos.
        sql = f"""SELECT * FROM product WHERE product_id={product_id};"""
        cur.execute(sql)
        
        # Comprobar si el producto ya existe y actualizar o insertar según corresponda.
        if(cur.fetchone() is not None):
            cur.execute(f"""UPDATE product
                        SET product_id={product_id}, name='{name}', description='{description}', company='{company}', price={price}, units={units} , subcategory_id={subcategory_id}, updated_at='{tiempo_actual}'
	                    WHERE product_id={product_id};""")
        else:
            cur.execute(f"INSERT INTO product (product_id, name, description, company, price, units, subcategory_id, created_at, updated_at) VALUES ({product_id}, '{name}', '{description}', '{company}', {price}, {units}, {subcategory_id}, '{tiempo_actual}', '{tiempo_actual}')")
        
        # Confirmar los cambios en la base de datos.
        conn.commit()
        
    except Exception as e:
        # Capturar errores e imprimir el mensaje de error.
        print(f'Error de conexión: {e}')
        
    finally:
        # Cerrar la conexión a la base de datos.
        conn.close()

# Función para cargar datos masivos desde un archivo CSV.
def pujarCSV(fitcherCSV): 
    try:
        # Establecer conexión con la base de datos.
        conn = clientPS.client()
        
        # Leer el archivo CSV especificado.
        df = pd.read_csv(fitcherCSV.file, header=0 )
        
        # Iterar sobre cada fila del DataFrame y procesar los datos.
        for index, row in df.iterrows():
            fila = row.to_dict()
            
            # Importar la categoría y la subcategoría si no existen.
            impCategoria(fila["id_categoria"], fila["nom_categoria"])
            impSubcategory(fila["id_subcategoria"], fila["nom_subcategoria"], fila["id_categoria"])
            
            # Crear o actualizar el producto en la base de datos.
            crearProducteCSV(fila["id_producto"], fila["nom_producto"], fila["descripcion_producto"], fila["companyia"], fila["precio"], fila["unidades"], fila["id_subcategoria"])

        # Confirmar la carga masiva de datos.
        conn.commit()
        return "Carga de datos masivos COMPLETADA!!!"
    
    except Exception as e:
        # Capturar errores e imprimir el mensaje de error.
        return f"Error al cargar datos masivos: {e}"
        
    finally:
        # Cerrar la conexión a la base de datos.
        conn.close()