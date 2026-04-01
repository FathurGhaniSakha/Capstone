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
)

var dbMaster *sql.DB
var dbReplica *sql.DB
var rdb *redis.Client
var ctx = context.Background()

// 1. Fungsi Database (Master & Replica)
func initDB() {
	var err error

	masterURL := os.Getenv("DB_MASTER_URL")
	replicaURL := os.Getenv("DB_REPLICA_URL")

	if masterURL == "" {
		masterURL = "postgres://user:pass@localhost:5432/cimb_db?sslmode=disable"
	}
	if replicaURL == "" {
		replicaURL = "postgres://user:pass@localhost:5433/cimb_db?sslmode=disable"
	}

	dbMaster, err = sql.Open("postgres", masterURL)
	if err != nil {
		log.Fatal("Gagal konek ke Master DB:", err)
	}

	dbReplica, err = sql.Open("postgres", replicaURL)
	if err != nil {
		log.Fatal("Gagal konek ke Replica DB:", err)
	}

	fmt.Println("Berhasil terhubung ke Master dan Replica Database!")
}

// 2. Fungsi Koneksi Redis Cache
func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	fmt.Println("Redis Client disiapkan!")
}

func main() {
	initDB()
	initRedis()

	app := fiber.New()

	// =====================================================================
	// FITUR PROTEKSI PEAK LOAD (TUGAS TEKNIK INFORMATIKA)
	// =====================================================================

	// 1. RECOVER: Mencegah server mati total/crash jika ada error "Panic"
	app.Use(recover.New())

	// 2. RATE LIMITER: Membatasi spam request dari user yang sama
	app.Use(limiter.New(limiter.Config{
		Max:        20,              // Maksimal 20 request
		Expiration: 1 * time.Minute, // Dalam waktu 1 menit
		KeyGenerator: func(c *fiber.Ctx) string {
			return c.IP() // Dibatasi berdasarkan IP Address nasabah
		},
		LimitReached: func(c *fiber.Ctx) error {
			// Jika melebihi batas, tolak dengan status HTTP 429 (Too Many Requests)
			fmt.Println("Peringatan: Ada indikasi spam/peak load dari IP:", c.IP())
			return c.Status(429).JSON(fiber.Map{
				"status":  "error",
				"message": "Sistem sedang sibuk. Terlalu banyak request, mohon tunggu sebentar.",
			})
		},
	}))

	// =====================================================================

	// ------------------------------------------------------------------------
	// ENDPOINT 1: BALANCE INQUIRY DENGAN REDIS CACHING
	// ------------------------------------------------------------------------
	app.Get("/users/:id/balance", func(c *fiber.Ctx) error {
		userID := c.Params("id")
		cacheKey := "balance_user_" + userID

		// Cek Redis
		cachedBalance, err := rdb.Get(ctx, cacheKey).Result()

		if err == redis.Nil {
			// Cache Miss
			fmt.Println("Cache Miss! Mengambil data dari Database Replica untuk User:", userID)

			// Simulasi Query ke dbReplica
			saldoDariDB := "Rp 5.000.000"

			// Simpan ke Redis (TTL 30 detik)
			rdb.Set(ctx, cacheKey, saldoDariDB, 30*time.Second)

			return c.Status(200).JSON(fiber.Map{
				"status":  "success",
				"source":  "Database Replica (Lambat)",
				"balance": saldoDariDB,
			})

		} else if err != nil {
			// Fallback jika Redis error
			return c.Status(500).SendString("Kesalahan Server Internal")
		}

		// Cache Hit
		fmt.Println("Cache Hit! Mengambil data langsung dari Redis untuk User:", userID)
		return c.Status(200).JSON(fiber.Map{
			"status":  "success",
			"source":  "Redis Cache (Sangat Cepat!)",
			"balance": cachedBalance,
		})
	})

	// ------------------------------------------------------------------------
	// ENDPOINT 2: CREATE TRANSACTION (MASTER DB)
	// ------------------------------------------------------------------------
	app.Post("/transactions", func(c *fiber.Ctx) error {
		// Logika insert ke dbMaster akan ditulis di sini
		return c.Status(201).JSON(fiber.Map{
			"status":  "success",
			"message": "Transaksi berhasil diproses via MASTER DB",
		})
	})

	fmt.Println("Server API Bank CIMB Niaga berjalan di Port 3005")
	log.Fatal(app.Listen(":3005"))
}