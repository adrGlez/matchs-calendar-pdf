'use client'

// import React from 'react'
import { Match } from '@/app/matches/page'

interface MatchesTableProps {
  matches: Match[]
}

const MatchesTable = ({ matches }: MatchesTableProps) => {

  return (
    <table className="mt-8 w-full table-auto border-collapse text-sm text-left dark:text-gray-400">
      <thead>
        <tr>
            <th className="px-4 py-2 border-b">Match</th>
            <th className="px-4 py-2 border-b">Date</th>
            <th className="px-4 py-2 border-b">Location</th>
          </tr>
      </thead>
      <tbody>
        {matches?.resultados?.map((match, index) => (
          <tr key={index} className="hover:bg-gray-100 dark:hover:bg-gray-700">
            <td className="px-4 py-2">{match.equipo}</td>
            <td className="px-4 py-2">{match.fecha_hora}</td>
            <td className="px-4 py-2">{match.visitante}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export default MatchesTable
