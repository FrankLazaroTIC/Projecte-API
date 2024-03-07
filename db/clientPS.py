import psycopg

### Connexio amb la BBDD
def client():
    
    conexio = """
            dbname=postgres
            user=user_postgres
            password=pass_postgres
            host=localhost
            port=5432
            """

    try:
        return psycopg.connect(conexio)
    
    except Exception as e:
        print(f"ERRORRRRR: {e}")


