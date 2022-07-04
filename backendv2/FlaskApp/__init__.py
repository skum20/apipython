
import logging
from flask import Flask, jsonify, request

import pymssql

def get_connection():
    try:
        conn = pymssql.connect(server='serverapipython.database.windows.net', user='angular', password='Crud246476', database='apipython') 
    
        return conn
    except Exception as ex:
        raise ex
class Pelicula():

    def __init__(self, id, titulo=None, puntuacion=None) -> None:
        self.id = id
        self.titulo = titulo
        self.puntuacion = puntuacion

    def to_JSON(self):
        return {
            'id': self.id,
            'titulo': self.titulo,
            'puntuacion': self.puntuacion
        }

class Pelicula2():

    def __init__(self,  titulo=None, puntuacion=None) -> None:

        self.titulo = titulo
        self.puntuacion = puntuacion

class PeliculaModel():

    @classmethod
    def get_movies(self):
        try:
            connection = get_connection()
            movies = []

            with connection.cursor() as cursor:
                cursor.execute("SELECT id, titulo, puntuacion FROM peliculas")
                resultset = cursor.fetchall()

                for row in resultset:
                    movie = Pelicula(row[0], row[1], row[2])
                    movies.append(movie.to_JSON())

            connection.close()
            return movies
        except Exception as ex:
            raise Exception(ex)

    @classmethod
    def get_movie(self, id):
        try:
            connection = get_connection()

            with connection.cursor() as cursor:
                cursor.execute("SELECT id, titulo, puntuacion FROM peliculas WHERE id = %s", (id))
                row = cursor.fetchone()

                movie = None
                if row != None:
                    movie = Pelicula(row[0], row[1], row[2])
                    movie = movie.to_JSON()

            connection.close()
            return movie
        except Exception as ex:
            raise Exception(ex)

    @classmethod
    def add_movie(self, movie):
        try:
            connection = get_connection()

            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO peliculas ( titulo, puntuacion) 
                                VALUES ( %s, %s )""", ( movie.titulo, movie.puntuacion))
                affected_rows = cursor.rowcount
                connection.commit()

            connection.close()
            return affected_rows
        except Exception as ex:
            raise Exception(ex)

    @classmethod
    def update_movie(self, movie):
        try:
            connection = get_connection()

            with connection.cursor() as cursor:
                cursor.execute("""UPDATE peliculas SET titulo = %s, puntuacion = %s 
                                WHERE id = %s""", (movie.titulo, movie.puntuacion,  movie.id))
                affected_rows = cursor.rowcount
                connection.commit()

            connection.close()
            return affected_rows
        except Exception as ex:
            raise Exception(ex)

    @classmethod
    def delete_movie(self, movie):
        try:
            connection = get_connection()

            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM peliculas WHERE id = %s", (movie.id,))
                affected_rows = cursor.rowcount
                connection.commit()

            connection.close()
            return affected_rows
        except Exception as ex:
            raise Exception(ex)

app = Flask(__name__)
@app.route("/peliculas" , methods=['GET'])
def get_movies():
    try:
        movies = PeliculaModel.get_movies()
        return jsonify(movies)
    except Exception as ex:
        return jsonify({'Mensaje': str(ex)}), 500

@app.route('/peliculas/<id>' , methods=['GET'])
def get_movie(id):
    try:
        movie = PeliculaModel.get_movie(id)
        
        if movie != None:
            return jsonify(movie)
        else:
            return jsonify({'Mensaje':"ID no encontrada"}), 404
    except Exception as ex:
        logging.info(movie)
        return jsonify({'Mensaje': str(ex)}), 500

@app.route('/peliculas', methods=['POST'])
def add_movie():
    try:
        titulo = request.json['titulo']
        puntuacion = int(request.json['puntuacion'])
 
        movie = Pelicula2( titulo, puntuacion)

        affected_rows = PeliculaModel.add_movie(movie)

        if affected_rows == 1:
            return jsonify({'Mensaje': "Exito"})
        else:
            return jsonify({'Mensaje': "Error al insertar"}), 500

    except Exception as ex:
        return jsonify({'Mensaje': str(ex)}), 500
@app.route('/peliculas/<id>', methods=['PUT'])
def update_movie(id):
    try: 
        titulo = request.json['titulo']
        puntuacion = int(request.json['puntuacion'])
        logging.info(id)
        logging.info(titulo)
        logging.info(puntuacion)
        movie = Pelicula(id, titulo, puntuacion)
        logging.info(movie)
        affected_rows = PeliculaModel.update_movie(movie)

        if affected_rows == 1:
            return jsonify({'Mensaje': "Actualizada"})
        else:
            return jsonify({'Mensaje': "Id no encontrado"}), 404

    except Exception as ex:
        return jsonify({'Mensaje': str(ex)}), 500

@app.route('/peliculas/<id>', methods=['DELETE'])
def delete_movie(id):
    try:
        movie = Pelicula(id)

        affected_rows = PeliculaModel.delete_movie(movie)

        if affected_rows == 1:
            return jsonify({'Mensaje': "Pelicula borrada"})
        else:
            return jsonify({'Mensaje': "ID no encontrada"}), 404

    except Exception as ex:
        return jsonify({'Mensaje': str(ex)}), 500
if __name__ == "__main__":
    app.run()