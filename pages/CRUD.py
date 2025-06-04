import streamlit as st
from CassandraDB import connect_to_cassandra
from Mongodb import connect_to_mongodb

st.set_page_config(page_title="CRUD Data", page_icon="📝")
st.markdown("Gunakan sidebar di kiri untuk navigasi ke halaman lain")

session = connect_to_cassandra("robd")
collection = connect_to_mongodb("ROBD", "Author")

# CRUD CASSANDRA
st.title("📦 CRUD Cassandra - Table: posts")
# === CREATE ===
st.header("📥 Tambah Data Baru")
author_id = st.number_input("author_id", step=1)
post_id = st.text_input("id")
author = st.text_input("author")
title = st.text_input("title")
flair = st.text_input("author_flair_text")
full_link = st.text_input("full_link")
created_utc = st.number_input("created_utc", step=1)
score = st.number_input("score", step=1)
num_comments = st.number_input("num_comments", step=1)
over_18 = st.checkbox("over_18")
removed_by = st.text_input("removed_by")
total_awards = st.number_input("total_awards_received", step=0.1)
awarders = st.text_input("awarders (comma separated)")

if st.button("➕ Tambahkan Data Cassandra"):
    try:
        session.execute("""
            INSERT INTO posts (author_id, id, author, author_flair_text, created_utc,
                                full_link, num_comments, over_18, removed_by, score,
                                title, total_awards_recevied, awarders)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (author_id, post_id, author, flair, created_utc, full_link, num_comments,
                over_18, removed_by, score, title, total_awards, awarders.split(',')))
        st.success("✅ Data berhasil ditambahkan!")
    except Exception as e:
        st.error(f"❌ Gagal menambahkan data: {e}")

# === SEARCH ===
st.header("🔍 Cari Post berdasarkan author_id dan id")
search_author_id = st.number_input("Masukkan author_id untuk cari", key="read_id", step=1)
search_post_id = st.text_input("Masukkan id post", key="read_post_id")
if st.button("Cari Post"):
    try:
        result = session.execute(
            "SELECT * FROM posts WHERE author_id=%s AND id=%s",
            (search_author_id, search_post_id)
        )
        rows = result.all()
        if rows:
            st.write(rows[0]._asdict())
        else:
            st.warning("⚠️ Data tidak ditemukan.")
    except Exception as e:
        st.error(f"❌ Gagal cari data: {e}")

# === UPDATE ===
st.header("🛠️ Update Post berdasarkan author_id dan id")
update_author_id = st.number_input("author_id yang ingin diupdate", key="update_author_id", step=1)
update_post_id = st.text_input("id post yang ingin diupdate", key="update_post_id")
field_to_update = st.selectbox("Pilih kolom yang ingin diupdate (string)", [
    "author", "author_flair_text", "title", "removed_by", "full_link"
])
new_value = st.text_input("Nilai baru")

if st.button("✅ Update Data Cassandra"):
    try:
        check = session.execute("SELECT * FROM posts WHERE author_id=%s AND id=%s",
                                (update_author_id, update_post_id)).all()
        if check:
            session.execute(
                f"UPDATE posts SET {field_to_update}=%s WHERE author_id=%s AND id=%s",
                (new_value, update_author_id, update_post_id)
            )
            st.success("✅ Data berhasil diperbarui")
        else:
            st.warning("⚠️ author_id atau id tidak ditemukan")
    except Exception as e:
        st.error(f"❌ Error saat update: {e}")

# === DELETE ===
st.header("🗑️ Hapus Post berdasarkan author_id dan id")
delete_author_id = st.number_input("author_id", key="delete_id", step=1)
delete_post_id = st.text_input("id post", key="delete_post_id")

if st.button("Hapus Post"):
    try:
        session.execute(
            "DELETE FROM posts WHERE author_id=%s AND id=%s",
            (delete_author_id, delete_post_id)
        )
        st.success("✅ Post berhasil dihapus.")
    except Exception as e:
        st.error(f"❌ Gagal hapus post: {e}")

# CRUD MONGODB
st.title("📦 CRUD MongoDB - Collection: Author")
# === CREATE ===
st.header("📥 Tambah Data Baru")
author_id = st.number_input("Author ID", min_value=1, step=1)
total_posts = st.number_input("Total Posts", min_value=0, step=1)
karma = st.number_input("Karma", min_value=0, step=1)
is_bot = st.selectbox("isBot", [True, False])
is_verified = st.selectbox("isVerified", [True, False])

if st.button("➕ Tambahkan Data MongoDB"):
    if collection.find_one({"author_id": int(author_id)}):
        st.warning("❗ Data dengan author_id ini sudah ada.")
    else:
        new_data = {
            "author_id": int(author_id),
            "total_posts": int(total_posts),
            "karma": int(karma),
            "isBot": is_bot,
            "isVerified": is_verified
        }
        result = collection.insert_one(new_data)
        st.success(f"✅ Data ditambahkan (_id: {result.inserted_id})")

# === SEARCH ===
st.header("🔍 Cari Data Berdasarkan Author ID")
search_id = st.number_input("Masukkan Author ID untuk Pencarian", step=1)
if st.button("Cari"):
    result = collection.find_one({"author_id": int(search_id)}, {"_id": 0})
    if result:
        st.json(result)
    else:
        st.info("Data tidak ditemukan.")

# === UPDATE ===
st.header("🛠️ Update Data Berdasarkan Author ID")
author_id = st.number_input("Masukkan Author ID yang ingin diupdate", step=1)

new_total_posts = st.number_input("Total Posts Baru", step=1)
new_karma = st.number_input("Karma Baru", step=1)
new_is_bot = st.selectbox("isBot Baru", [True, False])
new_is_verified = st.selectbox("isVerified Baru", [True, False])

if st.button("✅ Update Data MongoDB"):
    author = collection.find_one({"author_id": int(author_id)})

    if author:
        update_fields = {
            "total_posts": new_total_posts,
            "karma": new_karma,
            "isBot": new_is_bot,
            "isVerified": new_is_verified
        }

        result = collection.update_one(
            {"author_id": int(author_id)},
            {"$set": update_fields}
        )

        if result.modified_count > 0:
            st.success("✅ Data berhasil diperbarui.")
        else:
            st.info("⚠️ Data sudah sesuai, tidak ada yang berubah.")
    else:
        st.error("❌ author_id tidak ditemukan.")

# === DELETE ===
st.header("🗑️ Hapus Data Berdasarkan Author ID")
delete_id_mongo = st.number_input("Masukkan Author ID untuk Dihapus", step=1, key="delete_id_mongo")
if st.button("Hapus"):
    result = collection.delete_one({"author_id": int(delete_id_mongo)})
    if result.deleted_count:
        st.success("✅ Data berhasil dihapus.")
    else:
        st.warning("❗ Data tidak ditemukan / gagal dihapus.")

    