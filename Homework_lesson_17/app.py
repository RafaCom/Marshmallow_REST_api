from flask import Flask, request
from flask_restx import Api, Resource
from flask_sqlalchemy import SQLAlchemy
from marshmallow import Schema, fields
import sqlite3

app = Flask(__name__)
app.app_context().push()
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

api = Api(app)
api.app.config['RESTX_JSON'] = {'ensure_ascii': False, 'indent': 4}

# namespaces
movie_ns = api.namespace('movies')
director_ns = api.namespace('directors')
genre_ns = api.namespace('genres')


def conn(query):
    with sqlite3.connect('test.db') as connect:
        cursor = connect.cursor()
        result = cursor.execute(query).fetchall()
        return result


class Movie(db.Model):
    __tablename__ = 'movie'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255))
    description = db.Column(db.String(255))
    trailer = db.Column(db.String(255))
    year = db.Column(db.Integer)
    rating = db.Column(db.Float)
    genre_id = db.Column(db.Integer, db.ForeignKey("genre.id"))
    genre = db.relationship("Genre")
    director_id = db.Column(db.Integer, db.ForeignKey("director.id"))
    director = db.relationship("Director")


class MovieSchema(Schema):
    id = fields.Int()
    title = fields.Str()
    description = fields.Str()
    trailer = fields.Str()
    year = fields.Int()
    rating = fields.Float()
    genre_id = fields.Int()
    director_id = fields.Int()


class Director(db.Model):
    __tablename__ = 'director'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255))


class DirectorSchema(Schema):
    id = fields.Int()
    name = fields.Str()


class Genre(db.Model):
    __tablename__ = 'genre'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255))

class GenreSchema(Schema):
    name = fields.Str()


db.create_all()


def insert_db():
    movies = conn('''SELECT * FROM movie''')
    for movie in movies:
        movie_item = Movie(
            title=movie[1],
            description=movie[2],
            trailer=movie[3],
            year=movie[4],
            rating=movie[5],
            genre_id=movie[6],
            director_id=movie[7],
        )
        with db.session.begin():
            db.session.add(movie_item)

    genres = conn('''SELECT * FROM genre''')
    for genre in genres:
        genre_item = Genre(
            name=genre[1]
        )
        with db.session.begin():
            db.session.add(genre_item)

    directors = conn('''SELECT * FROM director''')
    for director in directors:
        director_item = Director(
            name=director[1]
        )
        with db.session.begin():
            db.session.add(director_item)

    # print(db.session.query(Movie).all())
    # print(db.session.query(Genre).all())
    # print(db.session.query(Director).all())

    # one_movie = db.session.query(Movie).filter(Movie.title == 'title').all()
    # print(one_movie)


# СОЗДАНИЕ СХЕМ И NAMESPACES
# Схема фильмов
movie_schema = MovieSchema()
movies_schema = MovieSchema(many=True)

# Схема режиссера
director_schema = DirectorSchema()

# Схема жанров
genre_schema = GenreSchema()


@movie_ns.route('/')
class MoviesView(Resource):

    def get(self):
        director_id = request.args.get('director_id')
        genre_id = request.args.get('genre_id')

        movies = db.session.query(Movie)

        if director_id:
            movies = movies.filter(Movie.director_id == director_id)
        if genre_id:
            movies = movies.filter(Movie.genre_id == genre_id)
        movies = movies.all()
        json_data = movies_schema.dump(movies)
        return json_data, 200


@movie_ns.route('/<int:mid>')
class MovieView(Resource):

    def get(self, mid):
        movie = db.session.query(Movie).filter(Movie.id == mid).first()
        json_data = movie_schema.dump(movie)
        return json_data, 200


@director_ns.route('/')
class DirectorViewPost(Resource):

    def post(self):
        req_json = request.json
        new_director = Director(**req_json)

        with db.session.begin():
            db.session.add(new_director)

        return "", 200


@director_ns.route('/<int:did>')
class DirectorView(Resource):

    def get(self, did):
        director = db.session.query(Director).get(did)
        return director_schema.dump(director), 200

    def put(self, did):
        req_json = request.json
        director = db.session.query(Director).get(did)

        director.name = req_json['name']

        db.session.add(director)
        db.session.commit()

        return "", 204

    def delete(self, did):
        director = db.session.query(Director).get(did)

        db.session.delete(director)
        db.session.commit()

        return "", 204


@genre_ns.route('/')
class GenreViewPost(Resource):

    def post(self):
        req_json = request.json

        new_genre = Genre(**req_json)

        with db.session.begin():
            db.session.add(new_genre)

        return "", 200


@genre_ns.route('/<int:gid>')
class GenreView(Resource):

    def get(self, gid):
        genre = db.session.query(Genre).get(gid)
        return genre_schema.dump(genre), 200

    def put(self, gid):
        req_json = request.json
        genre = db.session.query(Genre).get(gid)

        genre.name = req_json['name']

        db.session.add(genre)
        db.session.commit()

        return "", 204

    def delete(self, gid):
        genre = db.session.query(Genre).get(gid)

        db.session.delete(genre)
        db.session.commit()

        return "", 204


if __name__ == '__main__':
    insert_db()
    app.run()
