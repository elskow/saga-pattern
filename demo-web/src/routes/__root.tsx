import { createRootRoute, Outlet, HeadContent, Scripts, Link } from '@tanstack/react-router'
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
      <body className="flex min-h-full flex-col bg-[#f8f9fa] dark:bg-[#0a0a0a] text-foreground antialiased selection:bg-foreground selection:text-background relative">
        <div className="fixed inset-0 -z-10 h-full w-full bg-[radial-gradient(ellipse_100%_100%_at_50%_0%,rgba(0,0,0,0.04),transparent)] dark:bg-[radial-gradient(ellipse_100%_100%_at_50%_0%,rgba(255,255,255,0.04),transparent)]"></div>
        {children}
        <Scripts />
      </body>
    </html>
  )
}

function RootComponent() {
  return (
    <>
      <Navbar />

      <main className="flex-1 w-full max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-24 md:py-28">
        <Outlet />
      </main>

      <footer className="mt-auto w-full bg-muted/20 border-t border-border/40 pb-8 pt-12">
        <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
            <div className="flex flex-col md:flex-row justify-between items-center md:items-start gap-8">
                <div className="flex flex-col items-center md:items-start gap-2 max-w-sm text-center md:text-left">
                    <span className="text-xl font-bold tracking-tight text-foreground">
                        SagaStore
                    </span>
                    <p className="text-sm text-muted-foreground leading-relaxed">
                        Premium gadgets and technology for the modern professional. Upgrade your workflow today.
                    </p>
                </div>

                <div className="flex gap-8 text-sm font-semibold text-muted-foreground">
                    <Link to="/" className="hover:text-foreground transition-colors">
                        Shop
                    </Link>
                    <Link to="/orders" className="hover:text-foreground transition-colors">
                        Tracking
                    </Link>
                    <Link to="/admin" className="hover:text-foreground transition-colors">
                        Admin Console
                    </Link>
                </div>
            </div>

            <div className="mt-12 flex items-center justify-center border-t border-border/40 pt-8">
                <p className="text-xs font-medium text-muted-foreground">
                    © {new Date().getFullYear()} SagaStore. All rights reserved.
                </p>
            </div>
        </div>
      </footer>

      <Toaster position="bottom-left" theme="system" richColors closeButton />
    </>
  )
}
