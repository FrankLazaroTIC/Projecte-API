def product_schema(prod) -> dict:
    return {"id": str(prod[0]),
            "name": prod[1],
            "description": prod[2]}

def products_schema(prods) -> list:
    return [product_schema(prod) for prod in prods]