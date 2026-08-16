package main

import (
	"fmt"
	"kv-store/internal/store"
	"net/http"

	"strings"
)

func main() {
	kv := store.New()
	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")

		if key == "" {
			http.Error(w, "Missing key", http.StatusBadRequest)
			return
		}

		switch r.Method {
			
		case http.MethodGet:
			value, ok := kv.Get(key)
			if !ok {
				http.Error(w, "Key not found", http.StatusNotFound)
				return
			}
			w.Write([]byte(value))
		
		case http.MethodPut:
			var value string

			fmt.Fscan(r.Body, &value)
			kv.Set(key, value)
			w.WriteHeader(http.StatusNoContent)

		case http.MethodDelete:
			exists := kv.Delete(key)
			if !exists {
				http.Error(w, "Key not found", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)


		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	fmt.Println("Starting server on :8080")

	err := http.ListenAndServe(":8080", nil)

	if err != nil {
		fmt.Println("Error starting server:", err)
	}

}