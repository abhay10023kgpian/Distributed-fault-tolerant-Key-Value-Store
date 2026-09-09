import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/kv': 'http://localhost:8080',
      '/cluster': 'http://localhost:8080',
      '/nodes': 'http://localhost:8080',
      '/events': 'http://localhost:8080',
    },
  },
})
