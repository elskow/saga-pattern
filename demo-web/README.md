# SagaStore Web

TanStack Start storefront for the local saga comparison services.

## Development

```bash
npm install
npm run dev
```

Open `http://localhost:6173`.

The app expects the local saga services to be running on the repository defaults:

- Choreography: order `8081`, payment `8082`, inventory `8083`, shipping `8084`
- Orchestration: order `8091`, payment `8092`, inventory `8093`, shipping `8094`

## Useful Commands

```bash
npm run build
npm run test:routes
```

## Main Surfaces

- `/` storefront catalog
- `/checkout` purchase flow
- `/orders` customer order history
- `/admin` service and order operations
