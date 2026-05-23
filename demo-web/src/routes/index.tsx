import { createFileRoute } from '@tanstack/react-router'
import HomePageClient from '../HomePageClient'
import type { Pattern } from '@/types'

const initialPattern: Pattern = 'choreography'

export const Route = createFileRoute('/')({
  component: HomePage,
  loader: () => ({
    initialProducts: [],
    initialError: null,
    initialPattern,
  })
})

function HomePage() {
  const data = Route.useLoaderData()
  return <HomePageClient initialProducts={data.initialProducts} initialError={data.initialError} initialPattern={data.initialPattern} />
}
