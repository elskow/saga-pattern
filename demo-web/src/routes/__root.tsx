import { createRootRoute, Outlet, HeadContent, Scripts, Link, useLocation } from '@tanstack/react-router'
import type { ReactNode } from 'react'

import { Navbar } from "@/components/Navbar";
import { Logo } from "@/components/Logo";
import { Toaster } from "@/components/ui/sonner";

import appCss from "../globals.css?url";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      {
        charSet: 'utf-8',
      },
      {
        name: 'viewport',
        content: 'width=device-width, initial-scale=1',
      },
      {
        title: 'SagaStore | Laptops, phones & peripherals',
      },
      {
        name: 'description',
        content: 'Shop laptops, smartphones, and peripherals. Track orders from payment through delivery.',
      }
    ],
    links: [
      {
        rel: 'stylesheet',
        href: appCss,
      },
      {
        rel: 'icon',
        type: 'image/svg+xml',
        href: '/favicon.svg',
      },
    ],
  }),
  notFoundComponent: () => (
    <div className="flex flex-col items-center justify-center py-24 gap-4 text-center">
      <h1 className="text-3xl font-semibold tracking-tight text-foreground">Page not found</h1>
      <p className="text-sm text-muted-foreground max-w-sm">
        That page does not exist or was moved.
      </p>
      <Link
        to="/"
        className="inline-flex h-9 items-center rounded-md bg-primary px-4 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90"
      >
        Back to shop
      </Link>
    </div>
  ),
  shellComponent: RootDocument,
  component: RootComponent,
})

function RootDocument({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en" className="h-full">
      <head>
        <HeadContent />
      </head>
      <body className="flex min-h-full flex-col bg-background text-foreground antialiased selection:bg-primary/15 selection:text-foreground">
        {children}
        <Scripts />
      </body>
    </html>
  )
}

function RootComponent() {
  const pathname = useLocation().pathname;
  const isAuthRoute = pathname === '/login' || pathname === '/register' || pathname === '/admin/login';

  return (
    <>
      {!isAuthRoute && <Navbar />}

      <main className={!isAuthRoute ? "flex-1 w-full max-w-7xl mx-auto px-5 sm:px-8 pt-20 pb-12 md:pt-24 md:pb-16" : "flex-1 w-full flex flex-col"}>
        <Outlet />
      </main>

      {!isAuthRoute && (
        <footer className="mt-auto w-full border-t border-border bg-card">
          <div className="mx-auto flex max-w-7xl flex-col items-center justify-between gap-3 px-5 py-6 text-center sm:px-8 md:flex-row md:text-left">
            <div className="flex items-center gap-2">
              <Logo size={18} withWordmark={false} />
              <p className="text-xs text-muted-foreground">
                © {new Date().getFullYear()} SagaStore
              </p>
            </div>
            <div className="flex gap-6 text-sm text-muted-foreground">
              <Link to="/" className="transition-colors hover:text-primary">Shop</Link>
              <Link to="/orders" className="transition-colors hover:text-primary">Orders</Link>
            </div>
          </div>
        </footer>
      )}

      <Toaster
        position="bottom-right"
        theme="light"
        closeButton
        toastOptions={{
          classNames: {
            success: "!border-emerald-200 !bg-emerald-50 !text-emerald-800",
            error: "!border-red-200 !bg-red-50 !text-red-800",
          },
        }}
      />
    </>
  )
}
