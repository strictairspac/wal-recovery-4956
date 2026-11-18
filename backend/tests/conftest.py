import os, sys, tempfile
os.environ.setdefault('DATABASE_URL','sqlite+pysqlite:////tmp/wal-failover-tests.db')
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('REDIS_URL','redis://localhost:6379/0')
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.config import database
from src.config.database import Base
from src.models.entities import User, LogSet
from src.utils.security import hash_password
from scripts.seed_demo import RECORDS

@pytest.fixture()
def db(tmp_path):
    engine=create_engine(f'sqlite+pysqlite:///{tmp_path}/test.db',connect_args={'timeout':30})
    Base.metadata.create_all(engine)
    Session=sessionmaker(bind=engine,expire_on_commit=False)
    with Session() as session:
        for role in ('admin','operator','viewer'):
            session.add(User(email=role+'@haili.local',role=role,password_hash=hash_password('Recovery2026!')))
        session.add(LogSet(name='海砾采样账本-晚班',initial={'sample_A':100,'sample_B':50},records=RECORDS,description='test'))
        session.commit()
    yield Session
    engine.dispose()

@pytest.fixture()
def client(db):
    from fastapi.testclient import TestClient
    from src.main import app
    from src.config.database import get_db
    def override():
        with db() as session: yield session
    app.dependency_overrides[get_db]=override
    with TestClient(app) as c: yield c
    app.dependency_overrides.clear()
