from fastapi import FastAPI,Depends
from elasticsearch import Elasticsearch
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, select, text
from sqlalchemy.orm import  sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import redis,json
app = FastAPI()
# es = Elasticsearch()  # Assumes Elasticsearch is running locally

# redis 
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# mysql
import mysql.connector
host="172.16.12.6"
user="carpl_user"
password="carpl_user"
database="dictate"
SQLALCHEMY_DATABASE_URL = f"mysql+mysqlconnector://{user}:{password}@{host}:3306/{database}"
print(SQLALCHEMY_DATABASE_URL)
# Establish MySQL database connection
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, index=True)
    text = Column(String(25500))

Base.metadata.create_all(bind=engine)


class DocumentInput(BaseModel):
    text: str


# cors
origins = [
    "http://172.16.12.6",
    "http://172.16.12.6:3000",  # Replace with your frontend's URL
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.post("/insert_doc")
async def insert_document(document_input: DocumentInput):
    try:
        db = SessionLocal()
        document = Document(text=document_input.text)
        db.add(document)
        db.commit()
        db.refresh(document)
        return {"message": "Document inserted successfully", "document_id": document.id}
    except SQLAlchemyError as e:
        return {"message": "An error occurred", "error": str(e)}
    finally:
        db.close()


class DocumentOutput(BaseModel):
    id: int
    text: str
    highlighted_text: str



# @app.get("/search")
# async def search_documents(keyword: DocumentInput):
#     try:
#         db = SessionLocal()
#         stmt = select([Document])
#         stmt = stmt.where(text(f"text LIKE :keyword"))
#         stmt = stmt.params(keyword=f"%{keyword.text}%")
#         result = db.execute(stmt)
#         documents = [
#             DocumentOutput(
#                 id=row.id,
#                 text=row.text,
#                 highlighted_text=row.text.replace(keyword.text, f"<mark class='highlight'>{keyword.text}</mark>")
#             )
#             for row in result
#         ]
#         return documents
#     except SQLAlchemyError as e:
#         return {"message": "An error occurred", "error": str(e)}
#     finally:
#         db.close()

def get_redis_client():
    return redis_client

@app.post("/search")
async def search_documents(keyword: DocumentInput, redis_client: redis.Redis = Depends(get_redis_client)):
    cache_key = f"search:{keyword.text}"
    cached_result = redis_client.get(cache_key)
    if cached_result:
        documents = json.loads(cached_result)
    else:
        try:
            db = SessionLocal()
            stmt = select([Document])
            stmt = stmt.where(text(f"text LIKE :keyword"))
            stmt = stmt.params(keyword=f"%{keyword.text}%")
            result = db.execute(stmt)
            documents = [
                {
                    "id":row.id,
                    "text":row.text,
                    "highlighted_text":row.text.replace(keyword.text, f"<mark class='highlight'>{keyword.text}</mark>")
                }
                for row in result
            ]
            redis_client.set(cache_key, json.dumps(documents), ex=60)  # Cache result for 1 hour
        except SQLAlchemyError as e:
            return {"message": "An error occurred", "error": str(e)}
        finally:
            db.close()
    return documents