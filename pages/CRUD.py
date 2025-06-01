import streamlit as st
from Mongodb import connect_to_mongodb

st.set_page_config(page_title="CRUD Data", page_icon="📝")
st.markdown("Gunakan sidebar di kiri untuk navigasi ke halaman lain")

collection = connect_to_mongodb("ROBD", "Author")

# CRUD MONGODB
st.title("📦 CRUD MongoDB - Collection: Author")
# === CREATE ===
st.header("📥 Tambah Data Baru")
author_id = st.number_input("Author ID", min_value=1, step=1)
total_posts = st.number_input("Total Posts", min_value=0, step=1)
karma = st.number_input("Karma", min_value=0, step=1)
is_bot = st.selectbox("isBot", [True, False])
is_verified = st.selectbox("isVerified", [True, False])

if st.button("➕ Tambahkan Data"):
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

if st.button("✅ Update Data"):
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
delete_id = st.number_input("Masukkan Author ID untuk Dihapus", step=1, key="delete_id")
if st.button("Hapus"):
    result = collection.delete_one({"author_id": int(delete_id)})
    if result.deleted_count:
        st.success("✅ Data berhasil dihapus.")
    else:
        st.warning("❗ Data tidak ditemukan / gagal dihapus.")

    