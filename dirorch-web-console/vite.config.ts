import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    // Allow LAN hostname access when the dev server is started with `--host`.
    allowedHosts: true,
  },
})
