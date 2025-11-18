from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Variable
import pandas as pd
import time
import requests
import os
import ast
import json

from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Medium_to_Pinecone',
    default_args=default_args,
    description='Build a Medium Posting Search Engine using Pinecone',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['medium', 'pinecone', 'search-engine'],
) as dag:
    """
    DAG to build a Medium article search engine using Pinecone vector database
    """

    @task
    def download_data():
        """Download Medium dataset using requests"""
        data_dir = '/tmp/medium_data'
        os.makedirs(data_dir, exist_ok=True)

        file_path = f"{data_dir}/medium_data.csv"
        url = 'https://s3-geospatial.s3.us-west-2.amazonaws.com/medium_data.csv'
        r = requests.get(url, stream=True, timeout=120)
        r.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            line_count = sum(1 for _ in f)
        print(f"Downloaded file has {line_count} lines")

        return file_path

    @task
    def preprocess_data(data_path):
        """Clean and prepare data for embedding"""
        df = pd.read_csv(data_path)

        # Clean text columns
        df['title'] = df['title'].astype(str).fillna('')
        df['subtitle'] = df['subtitle'].astype(str).fillna('')

        # Create metadata 
        df['metadata'] = df.apply(
            lambda row: {'title': row['title'] + " " + row['subtitle']},
            axis=1
        )

        # Create string id
        df['id'] = df.reset_index(drop=True).index.astype(str)

        preprocessed_path = '/tmp/medium_data/medium_preprocessed.csv'
        df.to_csv(preprocessed_path, index=False)
        print(f"Preprocessed data saved to {preprocessed_path}")
        return preprocessed_path

    @task
    def create_pinecone_index():
        """Create or reset Pinecone index (serverless, 384-dim, dotproduct)"""
        api_key = Variable.get("pinecone_api_key")
        pc = Pinecone(api_key=api_key)

        spec = ServerlessSpec(
            cloud="aws",
            region="us-east-1"
        )
        index_name = 'semantic-search-fast'

        existing = [ix["name"] for ix in pc.list_indexes()]
        if index_name in existing:
            pc.delete_index(index_name)

        pc.create_index(
            name=index_name,
            dimension=384,            # dimensionality of MiniLM
            metric='dotproduct',      
            spec=spec
        )

        # Wait until the index is ready
        while not pc.describe_index(index_name).status['ready']:
            time.sleep(1)

        print(f"Pinecone index '{index_name}' created successfully")
        return index_name

    @task
    def generate_embeddings_and_upsert(data_path, index_name):
        """Generate embeddings, build df_upsert, and upsert via upsert_from_dataframe"""
        api_key = Variable.get("pinecone_api_key")
        df = pd.read_csv(data_path)

        # Ensure 'metadata' is a dict (CSV will load it as a string)
        def to_meta(x):
            if isinstance(x, dict):
                return x
            try:
                return ast.literal_eval(x)
            except Exception:
                return {'title': str(x)}

        df['metadata'] = df['metadata'].apply(to_meta)

        # Load model
        model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2', device='cpu')

        # df['values'] = embedding of metadata['title'] (384-dim list)
        df['values'] = df['metadata'].apply(lambda x: model.encode(x['title']).tolist())

        # Build df_upsert: ['id','values','metadata'] and ensure id is string
        df_upsert = df[['id', 'values', 'metadata']].copy()
        df_upsert['id'] = df_upsert['id'].astype(str)

        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)

        # Upsert directly from dataframe
        index.upsert_from_dataframe(df_upsert)

        print(f"Upserted {len(df_upsert)} records to Pinecone via upsert_from_dataframe")
        return index_name

    @task
    def test_search_query(index_name):
        """Search top 10 with metadata and values"""
        api_key = Variable.get("pinecone_api_key")

        model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2', device='cpu')
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)

        query = "what is ethics in AI"
        qv = model.encode(query).tolist()

        results = index.query(
            vector=qv,
            top_k=10,         
            include_metadata=True,
            include_values=True
        )

        print(f"Search results for query: '{query}'")

        # --- print top hits (works for both object and dict-like results) ---
        try:
            matches = results.matches or []
        except AttributeError:
            matches = results.get('matches', [])

        for m in matches:
            # Support both object and dict forms
            m_id = getattr(m, "id", m.get("id"))
            m_score = getattr(m, "score", m.get("score", 0.0))
            m_meta = getattr(m, "metadata", m.get("metadata", {})) or {}
            title = m_meta.get("title", "")
            print(f"ID: {m_id}, Score: {round(m_score or 0.0, 4)}, Title: {title[:80]}...")

        # --- pretty JSON for screenshots without failing on Pydantic models ---
        try:
            # pydantic v2 models (QueryResponse) support model_dump_json
            print(results.model_dump_json(indent=2))
        except Exception:
            try:
                print(json.dumps(results.model_dump(), indent=2, ensure_ascii=False))
            except Exception:
                # Last-resort manual conversion for very old/new variants
                serializable = {
                    "matches": [
                        {
                            "id": getattr(m, "id", m.get("id")),
                            "score": getattr(m, "score", m.get("score")),
                            "metadata": getattr(m, "metadata", m.get("metadata", {})),
                            "values": getattr(m, "values", m.get("values", None)),
                        }
                        for m in matches
                    ]
                }
                print(json.dumps(serializable, indent=2, ensure_ascii=False))
        return True

    # Task dependencies
    data_path = download_data()
    preprocessed_path = preprocess_data(data_path)
    index_name = create_pinecone_index()
    final_index_name = generate_embeddings_and_upsert(preprocessed_path, index_name)
    test_search_query(final_index_name)
