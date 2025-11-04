import { getScrapedMatches } from '@/api/route'
import { Suspense } from 'react'

interface Match {
  equipo: string;
  fecha_hora: string;
  visitante: string;
}

export default async function Page() {
  const matches: Match[] = await getScrapedMatches();

  return (
    <Suspense fallback={<div>Loading...</div>}>
    { matches.length > 0 && (
            <table className="mt-8 w-full table-auto border-collapse text-sm text-left bg-red-400 text-gray-600 dark:text-gray-400">
              <thead>
                <tr>
                  <th className="px-4 py-2 border-b">Match</th>
                  <th className="px-4 py-2 border-b">Date</th>
                  <th className="px-4 py-2 border-b">Location</th>
                </tr>
              </thead>
              <tbody>
                {matches.map((match, index) => (
                  <tr key={index} className="hover:bg-gray-100 dark:hover:bg-gray-700">
                    <td className="px-4 py-2">{match.equipo}</td>
                    <td className="px-4 py-2">{match.fecha_hora}</td>
                    <td className="px-4 py-2">{match.visitante}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
    </Suspense>
  )
}
