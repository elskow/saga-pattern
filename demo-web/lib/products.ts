import { CatalogProduct, Pattern } from "@/types";
import { createServerFn } from "@tanstack/react-start";
import { getServiceUrl } from "./effect-services";

interface BackendCatalogProduct {
  productId: string;
  name: string;
  description: string;
  price: number;
  image: string;
  category: string;
  stock: number;
  reserved: number;
  available: number;
}

function mapCatalogProduct(product: BackendCatalogProduct): CatalogProduct {
  return {
    ...product,
    id: product.productId,
    stock: product.available,
  };
}

async function fetchCatalogFromUrl(url: string): Promise<CatalogProduct[]> {
  const res = await fetch(`${url}/api/catalog`, {
    signal: AbortSignal.timeout(10000),
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`${url}: ${res.status}`);
  }

  const data: unknown = await res.json();
  if (!Array.isArray(data)) {
    throw new Error(`${url}: invalid payload`);
  }

  return data.map((product) => mapCatalogProduct(product as BackendCatalogProduct));
}

export async function fetchLiveCatalog(pattern: Pattern): Promise<CatalogProduct[]> {
  return fetchCatalogFromUrl(getServiceUrl("inventory", pattern, false));
}

export const fetchLiveCatalogServer = createServerFn({ method: "POST" })
  .inputValidator((data: Pattern) => data)
  .handler(async ({ data }: { data: Pattern }): Promise<CatalogProduct[]> => {
    try {
      return await fetchCatalogFromUrl(getServiceUrl("inventory", data, true));
    } catch (error) {
      throw new Error(
        `Live ${data} catalog unavailable. ${error instanceof Error ? error.message : "unknown error"}`
      );
    }
  });
