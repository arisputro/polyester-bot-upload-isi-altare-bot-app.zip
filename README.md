# Polyester testnet quoting bot — Altare App Hosting

Quoting bot BTC-USDT (post-only, 2 sisi) untuk testnet Polyester.
Jalan terus 24/7 sebagai Altare app (worker, tanpa port).

## Deploy di Altare (console.altare.gg)

1. Buat repo GitHub **publik** baru, upload semua file folder ini.
2. Di console Altare: **Apps → Create app → GitHub repository**,
   hubungkan GitHub, pilih repo ini.
3. Di langkah **Build and run**, pastikan:
   - Runtime: **Python 3.12**
   - Start command: `python quote_bot.py`
   - (Procfile di repo ini sudah mengaturnya.)
4. Di **Deployments → Settings → Variables**, isi (tersimpan terenkripsi):
   - `POLYESTER_API_KEY_ID` — Key ID Subaccount (`ak_...`)
   - `POLYESTER_API_KEY_PRIVATE` — private key 64 hex (key Subaccount)
   - `POLYESTER_SUBACCOUNT_ID` — ID Subaccount bot
   - Opsional: `QB_NOTIONAL` (default 20), `QB_MAX_INVENTORY` (default 100),
     `QB_EQUITY_STOP_PCT` (default 10).
5. Pilih region + size terkecil, **Create app**. Deploy pertama jalan langsung.
   Cek log di console: quote terpasang, fill, dan ringkasan per jam.

## Sebelum deploy

- Di dashboard Polyester: buat **Subaccount** khusus bot, buat API key
  Subaccount dengan policy **TRADE_SPOT di BTC-USDT** (+ Balances),
  danai Subaccount dari Main via internal transfer
  (saran: ~$150 USDT + ~0.002 BTC, dari dana paper klaim harian).
- **Jangan pernah** menulis key ke repo/file di sini — key hanya diisi
  di Variables Altare.

## Perilaku bot

- Quote ~$20/sisi, requote saat geser ≥2 tick, maks inventaris ±$100.
- Berhenti otomatis + batalkan semua order bila equity turun >10%
  dari awal sesi. Dead-man switch 30 dtk (heartbeat 10 dtk).
- Testnet saja, dana paper. Live order/stream belum pernah dites
  sebelum deploy ini — awasi log di menit-menit pertama.
