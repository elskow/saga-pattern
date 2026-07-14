import { createRootRoute, Outlet, HeadContent, Scripts, Link, useLocation } from '@tanstack/react-router'
import type { ReactNode } from 'react'

import { Navbar } from "@/components/Navbar";
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
        title: 'SagaStore | Premium Tech & Gadgets',
      },
      {
        name: 'description',
        content: 'Shop the latest high-end laptops, smartphones, and premium peripherals.',
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
    <div className="flex flex-col items-center justify-center py-32 gap-4">
      <h1 className="text-4xl font-bold tracking-tight">404 - Not Found</h1>
      <p className="text-muted-foreground">The page you're looking for doesn't exist.</p>
      <Link to="/" className="text-primary hover:underline">Go back home</Link>
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
      <body className="flex min-h-full flex-col bg-[#f8f9fa] text-foreground antialiased selection:bg-foreground selection:text-background relative">
        <div className="fixed inset-0 -z-10 h-full w-full bg-[radial-gradient(ellipse_100%_100%_at_50%_0%,rgba(0,0,0,0.04),transparent)]"></div>
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

      <main className={!isAuthRoute ? "flex-1 w-full max-w-7xl mx-auto px-6 sm:px-8 py-24 md:py-28" : "flex-1 w-full flex flex-col"}>
        <Outlet />
      </main>

      {!isAuthRoute && (
        <footer className="mt-auto w-full border-t border-border/50 bg-background/50 backdrop-blur-lg">
          <div className="mx-auto flex max-w-7xl flex-col items-center justify-between gap-4 px-6 py-8 text-center sm:px-8 md:flex-row md:text-left">
            <p className="text-xs font-medium text-muted-foreground">
              © {new Date().getFullYear()} SagaStore. All rights reserved.
            </p>
            <div className="flex gap-6 text-sm font-medium text-muted-foreground">
              <Link to="/" className="hover:text-foreground transition-colors">Shop</Link>
              <Link to="/orders" className="hover:text-foreground transition-colors">Orders</Link>
              <Link to="/about" className="hover:text-foreground transition-colors">About</Link>
            </div>
          </div>
        </footer>
      )}

      <Toaster position="bottom-left" theme="light" richColors closeButton />
    </>
  )
}
