package main

import (
	bitcask "Bitcask_go"
	"Bitcask_go/config"
	"Bitcask_go/util"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

func init() {
	var err error
	cfg := config.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	cfg.DataDir = dir
	db, err = bitcask.Open(cfg)
	if err != nil {
		panic(fmt.Sprintf("failed to open db:%v", err))
	}
}

func HandlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put value in db:%v\n", err)
			return
		}
	}
}

func HandleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil && err != util.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func main() {
	//register func
	http.HandleFunc("/bitcask/put", HandlePut)
	http.HandleFunc("/bitcask/get", HandleGet)
	// start http service
	http.ListenAndServe("localhost:8080", nil)
}
