import glob

# Aranan ve değiştirilecek metinler
search_str = "from fakestore import FakeRedis"
replace_str = "from fakeredis.aioredis import FakeRedis"

# tests/core klasöründeki tüm Python dosyalarını bul
files = glob.glob("tests/core/*.py")

degistirilen_dosya_sayisi = 0

for file in files:
    # Dosyayı oku
    with open(file, "r", encoding="utf-8") as f:
        content = f.read()

    # Eğer hatalı import varsa değiştir ve kaydet
    if search_str in content:
        new_content = content.replace(search_str, replace_str)
        with open(file, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"✅ Düzeltildi: {file}")
        degistirilen_dosya_sayisi += 1

print(f"\nİşlem tamamlandı! Toplam {degistirilen_dosya_sayisi} dosya güncellendi.")