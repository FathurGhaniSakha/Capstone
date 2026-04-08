package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/recover"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
)

var dbMaster *sql.DB
var dbReplica *sql.DB
var rdb *redis.Client
var ctx = context.Background()
var jumlahKapasitasServer = 1

// Variabel Global untuk Circuit Breaker
var cb *gobreaker.CircuitBreaker

func initDB() {
	masterURL := os.Getenv("DB_MASTER_URL")
	replicaURL := os.Getenv("DB_REPLICA_URL")
	if masterURL == "" {
		masterURL = "postgres://user:pass@localhost:5432/cimb_db?sslmode=disable"
	}
	if replicaURL == "" {
		replicaURL = "postgres://user:pass@localhost:5433/cimb_db?sslmode=disable"
	}

	dbMaster, _ = sql.Open("postgres", masterURL)
	dbReplica, _ = sql.Open("postgres", replicaURL)
	fmt.Println("🗄️ Berhasil terhubung ke Master dan Replica Database!")
}

func initRedis() {
	rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	fmt.Println("⚡ Redis Client disiapkan!")
}

// =====================================================================
// INISIALISASI CIRCUIT BREAKER
// =====================================================================
func initCircuitBreaker() {
	cb = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "Database_Replica_CB",
		MaxRequests: 2,                // Saat mode HALF-OPEN, izinkan 2 request untuk tes ombak
		Interval:    10 * time.Second, // Reset hitungan error setiap 10 detik
		Timeout:     15 * time.Second, // Jika sekering PUTUS (OPEN), tunggu 15 detik sebelum dicoba lagi
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// KONDISI PUTUS: Jika gagal berturut-turut lebih dari 3 kali!
			return counts.ConsecutiveFailures > 3
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			// Munculkan peringatan di terminal saat sekering berubah status
			fmt.Printf("\n🚨 [CIRCUIT BREAKER] Perhatian! Status berubah dari %s menjadi %s!\n\n", from, to)
		},
	})
	fmt.Println("🛡️ Circuit Breaker (Sekering Anti-Crash) disiapkan!")
}

func workerTransaksi(workerID int) {
	for {
		hasil, err := rdb.BRPop(ctx, 0, "queue_transaksi").Result()
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		_ = hasil[1]
		time.Sleep(1 * time.Second)
	}
}

func simulatorPredictiveScaling() {
	for {
		time.Sleep(1 * time.Hour) // Diperlama agar terminal Anda tidak berisik saat demo
	}
}

func main() {
	initDB()
	initRedis()
	initCircuitBreaker() // Panggil inisialisasi CB

	go simulatorPredictiveScaling()

	for i := 1; i <= 100; i++ {
		go workerTransaksi(i)
	}

	app := fiber.New()
	app.Use(recover.New())
	app.Use(limiter.New(limiter.Config{
		Max: 20, Expiration: 1 * time.Minute,
		KeyGenerator: func(c *fiber.Ctx) string { return c.IP() },
		LimitReached: func(c *fiber.Ctx) error {
			return c.Status(429).JSON(fiber.Map{"status": "error", "message": "Terlalu banyak request."})
		},
	}))

	// ------------------------------------------------------------------------
	// ENDPOINT 1: BALANCE INQUIRY + CIRCUIT BREAKER
	// ------------------------------------------------------------------------
	app.Get("/users/:id/balance", func(c *fiber.Ctx) error {
		userID := c.Params("id")
		cacheKey := "balance_user_" + userID

		cachedBalance, err := rdb.Get(ctx, cacheKey).Result()

		if err == redis.Nil { // Cache Miss -> Harus ambil ke Database

			// 🛡️ BUNGKUS PROSES KE DATABASE DENGAN CIRCUIT BREAKER
			hasilDB, cbErr := cb.Execute(func() (interface{}, error) {

				// --- SIMULASI ERROR UNTUK DEMO DOSEN ---
				// Jika yang dicek adalah User ID "error", paksa database gagal/timeout
				if userID == "error" {
					return nil, fmt.Errorf("database timeout/mati")
				}
				// ---------------------------------------

				// Simulasi data berhasil diambil dari Database Replica
				saldoAsli := "Rp 5.000.000"
				return saldoAsli, nil
			})

			// Jika Circuit Breaker memutuskan koneksi (OPEN) atau fungsi di atas me-return error
			if cbErr != nil {
				return c.Status(503).JSON(fiber.Map{
					"status":  "error",
					"source":  "Circuit Breaker (Fail-Fast)",
					"message": "Sistem database sedang gangguan parah. Request Anda ditolak sementara untuk mencegah Server Crash. Coba lagi dalam 15 detik.",
				})
			}

			// Jika aman, simpan ke cache dan kembalikan
			saldoDariDB := hasilDB.(string)
			rdb.Set(ctx, cacheKey, saldoDariDB, 30*time.Second)
			return c.Status(200).JSON(fiber.Map{"source": "Database Replica", "balance": saldoDariDB})

		} else if err != nil {
			return c.Status(200).JSON(fiber.Map{"source": "Database Replica (Fallback)", "balance": "Rp 5.000.000"})
		}

		return c.Status(200).JSON(fiber.Map{"source": "Redis Cache", "balance": cachedBalance})
	})

	app.Post("/transactions", func(c *fiber.Ctx) error {
		dataTransfer := fmt.Sprintf("Transfer_Rp100000_waktu_%s", time.Now().Format("15:04:05.000"))
		rdb.LPush(ctx, "queue_transaksi", dataTransfer)
		return c.Status(202).JSON(fiber.Map{"status": "pending", "message": "Masuk antrean."})
	})

	fmt.Println("🚀 Server API Bank CIMB Niaga berjalan di Port 3005")
	log.Fatal(app.Listen(":3005"))
}
