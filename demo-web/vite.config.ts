import { defineConfig } from 'vite'
import { devtools } from '@tanstack/devtools-vite'
import { tanstackStart } from '@tanstack/react-start/plugin/vite'
import viteReact from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { nitro } from 'nitro/vite'

const SERVICES = [
  { prefix: "choreography", name: "payment", port: 8082 },
  { prefix: "choreography", name: "inventory", port: 8083 },
  { prefix: "choreography", name: "shipping", port: 8084 },
  { prefix: "choreography", name: "order", port: 8081 },
  { prefix: "orchestration", name: "payment", port: 8092 },
  { prefix: "orchestration", name: "inventory", port: 8093 },
  { prefix: "orchestration", name: "shipping", port: 8094 },
  { prefix: "orchestration", name: "order", port: 8091 },
];

const proxies: Record<string, any> = {};

SERVICES.forEach(({ prefix, name, port }) => {
  const envVar = `${prefix.toUpperCase()}_${name.toUpperCase()}_URL`;
  const target = process.env[envVar] || `http://localhost:${port}`;
  
  const routeBase = name === "order" ? `/proxy/${prefix}` : `/proxy/${prefix}-${name}`;
  
  proxies[`^${routeBase}/.*`] = {
    target,
    changeOrigin: true,
    rewrite: (path: string) => path.replace(new RegExp(`^${routeBase}`), ""),
  };

  proxies[`/proxy/health/${prefix}-${name}`] = {
    target,
    changeOrigin: true,
    rewrite: () => "/health",
  };
});

const config = defineConfig({
  resolve: { tsconfigPaths: true },
  plugins: [
    devtools(),
    nitro({ rollupConfig: { external: [/^@sentry\//] } }),
    tailwindcss(),
    tanstackStart({ devRenderMode: "client" }),
    viteReact(),
  ],
  server: { proxy: proxies },
})

export default config

