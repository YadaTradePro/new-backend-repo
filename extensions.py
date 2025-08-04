# extensions.py
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_jwt_extended import JWTManager # اضافه شدن JWTManager

db = SQLAlchemy()
bcrypt = Bcrypt()
jwt = JWTManager() # اضافه شدن JWTManager
