import streamlit as st
from CassandraDB import connect_to_cassandra
from Mongodb import connect_to_mongodb
import json
import pandas as pd
import time

st.title("🔍 Multi-Database Query Interface")

# --- Cassandra Query Input ---
st.header("1️⃣ Query ke Cassandra (CQL)")
cassandra_query = st.text_area("Masukkan query CQL untuk Cassandra:", "SELECT * FROM posts LIMIT 3;")
if st.button("Jalankan Query Cassandra"):
    try:
        cassandra_session = connect_to_cassandra('robd')
        rows = cassandra_session.execute(cassandra_query)
        cassandra_result = [dict(row._asdict()) for row in rows]
        st.write("Hasil Cassandra:")
        st.dataframe(cassandra_result)
    except Exception as e:
        st.error(f"Error Cassandra: {e}")

# --- MongoDB Query Input ---
st.header("2️⃣ Query ke MongoDB (JSON)")
mongo_query_input = st.text_area("Masukkan query JSON MongoDB:", '{"isBot":true}')
if st.button("Jalankan Query MongoDB"):
    try:
        mongo_collection = connect_to_mongodb("ROBD", "Author")
        mongo_query = json.loads(mongo_query_input)
        mongo_result = list(mongo_collection.find(mongo_query, {"_id": 0}))
        st.write("Hasil MongoDB:")
        st.dataframe(mongo_result)
    except Exception as e:
        st.error(f"Error MongoDB: {e}")

# --- Join Data ---
# --- Fungsi untuk query MongoDB ---
def run_mongo_query(query_str):
    try:
        start = time.time()
        query_dict = json.loads(query_str)
        mongo_collection = connect_to_mongodb("ROBD", "Author")
        result = list(mongo_collection.find(query_dict, {"_id": 0}))
        end = time.time()
        st.success(f"Waktu eksekusi MongoDB: {end - start:.4f} detik")
        return pd.DataFrame(result)
    except Exception as e:
        st.error(f"MongoDB error: {e}")
        return pd.DataFrame()

# --- Fungsi untuk query Cassandra ---
def run_cassandra_query(query_str):
    try:
        cassandra_session = connect_to_cassandra('robd')
        start = time.time()
        rows = cassandra_session.execute(query_str)
        df = pd.DataFrame(rows.all())
        end = time.time()
        st.success(f"Waktu eksekusi Cassandra: {end - start:.4f} detik")
        return df
    except Exception as e:
        st.error(f"Cassandra error: {e}")
        return pd.DataFrame()

# --- UI Streamlit ---
st.title("🧠 Gabungkan Data dari Cassandra & MongoDB")

# 1. Input query Cassandra
cassandra_query = st.text_area("1️⃣ Masukkan query Cassandra (CQL):", "SELECT * FROM posts;")

# 2. Input query MongoDB
mongo_query = st.text_area("2️⃣ Masukkan query MongoDB (JSON):", '{"isVerified":true}')

# 3. Tombol untuk menjalankan
if st.button("🔁 Jalankan dan Gabungkan"):
    df_cassandra = run_cassandra_query(cassandra_query)
    df_mongo = run_mongo_query(mongo_query)

    if not df_cassandra.empty and not df_mongo.empty:
        # 4. Gabungkan dua df berdasarkan 'author_id'
        df_merged = pd.merge(df_cassandra, df_mongo, on="author_id", how="inner")
        st.subheader("🔗 Data Gabungan berdasarkan `author_id`")
        st.dataframe(df_merged)
    else:
        st.warning("Salah satu data kosong, pastikan query menghasilkan data.")

st.subheader("⚙️ Indexing")
index_field_cassandra = st.text_input("Masukkan field Cassandra untuk indexing:", "author")
st.text("*author_id sudah menjadi partition key pada cassandra")
index_field_mongo = st.text_input("Masukkan field MongoDB untuk indexing:", "author_id")


if st.button("🔧 Lakukan Indexing"):
     # Cassandra Index
    try:
        cassandra_session = connect_to_cassandra("robd")
        create_index_query = f"CREATE INDEX IF NOT EXISTS ON posts ({index_field_cassandra});"
        cassandra_session.execute(create_index_query)
        st.success(f"Index Cassandra dibuat pada field: {index_field_cassandra}")
    except Exception as e:
        st.error(f"Gagal membuat index Cassandra: {e}")

        # MongoDB Index
    try:
        mongo_collection = connect_to_mongodb("ROBD", "Author")
        mongo_collection.create_index([(index_field_mongo, 1)])
        st.success(f"Index MongoDB dibuat pada field: {index_field_mongo}")
    except Exception as e:
        st.error(f"Gagal membuat index MongoDB: {e}")
    

# Tombol Hapus Index
if st.button("🧹 Hapus Index"):
    # Hapus Cassandra Index
    cassandra_keyspace = "robd"
    cassandra_table = "posts"
    try:
        cassandra_session = connect_to_cassandra("robd")
        index_name = f"{cassandra_keyspace}.{cassandra_table}_{index_field_cassandra}_idx"
        drop_query = f"DROP INDEX IF EXISTS {index_name};"
        cassandra_session.execute(drop_query)
        st.success(f"🗑️ Index Cassandra '{index_name}' dihapus")
    except Exception as e:
        st.error(f"❌ Gagal menghapus index Cassandra: {e}")

    # Hapus MongoDB Index
    try:
        mongo_collection = connect_to_mongodb("ROBD", "Author")
        indexes = mongo_collection.index_information()
        for name in indexes:
            if index_field_mongo in indexes[name]['key'][0]:
                mongo_collection.drop_index(name)
                st.success(f"🗑️ Index MongoDB '{name}' dihapus")
                break
        else:
            st.info("ℹ️ Tidak ditemukan index MongoDB dengan field tersebut.")
    except Exception as e:
        st.error(f"❌ Gagal menghapus index MongoDB: {e}")