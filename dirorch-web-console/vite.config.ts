import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    // Allow LAN hostname access when the dev server is started with `--host`.
    allowedHosts: true,
    proxy: {
      '/workflow': 'http://127.0.0.1:8000',
      '/status': 'http://127.0.0.1:8000',
      '/entity': 'http://127.0.0.1:8000',
      '/file': 'http://127.0.0.1:8000',
    },
  },
})
