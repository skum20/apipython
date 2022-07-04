
import json
import logging
import pymssql 
import azure.functions as func
import sys

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    conn = pymssql.connect(server='localhost', user='root', password='', database='apipython') 
    cursor = conn.cursor()
    #Funcion para crear
    def post_query(query,cursor):
        try:
            cursor.execute(query)
            conn.commit()
            logging.info("Query successful")
        except:
            logging.info(f"Error",sys.exc_info()[0])
    #Funcion para ler
    def read_query(query, cursor):
        
        try:
            result = None
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except ConnectionAbortedError:
            logging.info('Error de conexion')
            return func.HttpResponse("Fallo la conexion",status_code=404)
    def readOneQuery(query, cursor):
        
        try:
            result = None
            cursor.execute(query)
            result = cursor.fetchone()
            return result
        except ConnectionAbortedError:
            logging.info('Error de conexion')
            return func.HttpResponse("Fallo la conexion",status_code=404)

    #Listar todos
    if req.method=="GET" and req.route_params.get("param1")=="get"  and req.route_params.get("param2")==None:
        query="SELECT id, titulo, puntuacion FROM dbo.peliculas"
        result=read_query(query,cursor)
        respuesta=[]
        i=0
        for val in result:
            movie = {"ID":val[0],"Titulo":val[1],"Puntuacion":val[2]}
            respuesta.append(movie)
        s1 = json.dumps(respuesta)
        return func.HttpResponse(s1,status_code=200)
    #Crear
    elif req.method=="POST" and req.route_params.get("param1")=="post":
        body=req.get_json()
        titulo=str(body.get('Titulo'))
        puntuacion=int(body.get('Puntuacion'))
        query=f"INSERT INTO peliculas (titulo, puntuacion) VALUES ('{titulo}', '{puntuacion}');"
        result=post_query(query,cursor)
        return func.HttpResponse("Pelicula creada",status_code=200)
    #Obtener por id
    elif req.method=="GET" and req.route_params.get("param1")=="getbyid" :
        idpeli=req.route_params.get("param2")
        if idpeli==None:
            return func.HttpResponse("Ingresa un ID valido",status_code=404)
        elif idpeli is not None:
            query=f"SELECT id, titulo, puntuacion FROM dbo.peliculas WHERE id= '{idpeli}'"
            result=readOneQuery(query,cursor)
            if result==None:
                return func.HttpResponse("ID no encontrado",status_code=404)
            elif result is not []:
                logging.info(result)
                result2 = {"ID":result[0],"Titulo":result[1],"Puntuacion":result[2]}
                result3=json.dumps(result2)
                return func.HttpResponse(result3,status_code=200)
    #Borrar
    
    elif req.method=="DELETE" and req.route_params.get("param1")=="delete":
        body=req.get_json()
        idpeli=int(body.get('ID'))
        query=f"SELECT id FROM dbo.peliculas WHERE id= '{idpeli}'"
        result=readOneQuery(query,cursor)
        if idpeli==None or result==None:
            return func.HttpResponse("ID no encontrado",status_code=404)
        elif idpeli is not None:
            query=f"delete from dbo.peliculas where id='{idpeli}';"
            result=post_query(query,cursor)
            return func.HttpResponse("Pelicula borrada",status_code=200)
    #Actualizar  

    elif req.method=="POST" and req.route_params.get("param1")=="update":
        body=req.get_json()
        idpeli=(body.get('ID'))
        query=f"SELECT id FROM dbo.peliculas WHERE id= '{idpeli}'"
        result=readOneQuery(query,cursor)
        if idpeli==None or result==None:
            return func.HttpResponse("ID no encontrado",status_code=404)
        elif idpeli is not None:
            titulo=str(body.get('Titulo'))
            puntuacion=int(body.get('Puntuacion'))
            query=f"UPDATE peliculas SET titulo = '{titulo}', puntuacion = '{puntuacion}' WHERE id='{idpeli}'; "
            result=post_query(query,cursor)
            return func.HttpResponse("Pelicula actualizada",status_code=200)
    
    else:
        return func.HttpResponse(status_code=404)
        

    
