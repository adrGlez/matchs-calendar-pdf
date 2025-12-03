'use client'

import { getScrapedMatches } from '@/api/route'
import MatchesTable from '@/components/matchesTable'
import { Suspense, useEffect, useState } from 'react'

export interface Match {
  equipo: string;
  fecha_hora: string;
  visitante: string;
}

export default function Page() {
  const [matches, setMatches] = useState<Match[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const runScraping = async () => {
      try {
        setIsLoading(true)
        const data = await getScrapedMatches()
        setMatches(data)

      } catch (e: any) {
          setError(e?.message ?? 'Ha ocurrido un error');
      } finally {
        setIsLoading(false)
      }
    }

    runScraping()
  }, [])

  if (isLoading) {
    return (
      <main className="min-h-screen flex items-center justify-center">
        <div className="bg-white rounded-xl px-6 py-4 shadow">
          <p className="mb-2 font-medium text-center">
            Estamos haciendo el scraping…
          </p>
          <p className="text-sm text-gray-600 text-center">
            Esto puede tardar unos segundos. No cierres la página.
          </p>
        </div>
      </main>
    )
  }

  if (error) {
    return (
      <main className="min-h-screen flex flex-col items-center justify-center">
        <p className="text-red-500 mb-4">{error}</p>
        <a
          href="/"
          className="px-4 py-2 rounded border border-gray-300 text-sm"
        >
          Volver a la landing
        </a>
      </main>
    );
  }

  return (
      <MatchesTable matches={matches} />
  )
}
