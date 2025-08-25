import os
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///customer_reviews.db')

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class DatabaseManager:
    
    def __init__(self):
        self.engine = engine
        self.SessionLocal = SessionLocal
        self.Base = Base
    
    def create_tables(self):
        Base.metadata.create_all(bind=engine)
    
    def drop_tables(self):
        Base.metadata.drop_all(bind=engine)
    
    def get_session(self):
        return SessionLocal()
    
    def close_session(self, session):
        session.close()
    
    def get_engine(self):
        return self.engine