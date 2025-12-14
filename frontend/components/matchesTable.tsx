'use client'

import React from 'react'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  ColumnDef,
} from '@tanstack/react-table'
import { Match } from '@/app/matches/page'

interface MatchesTableProps {
  matches: { results: Match[] }
}

const MatchesTable = ({ matches }: MatchesTableProps) => {
  // Estado local para poder editar y reordenar
  const [data, setData] = React.useState<Match[]>(matches.results ?? [])

  // Si cambian los props desde arriba, sincronizamos
  React.useEffect(() => {
    setData(matches.results ?? [])
  }, [matches])

  // ---- Edición de celdas ----
  const handleCellChange = React.useCallback(
    (rowIndex: number, key: keyof Match, value: string) => {
      setData(prev => {
        const updated = [...prev]
        if (!updated[rowIndex]) return prev
        updated[rowIndex] = {
          ...updated[rowIndex],
          [key]: value,
        }
        return updated
      })
    },
    []
  )

  // ---- Drag & drop para reordenar ----
  const [dragIndex, setDragIndex] = React.useState<number | null>(null)

  const handleDragStart = React.useCallback((index: number) => {
    setDragIndex(index)
  }, [])

  const handleDragOver = React.useCallback(
    (e: React.DragEvent<HTMLTableRowElement>) => {
      e.preventDefault() // necesario para permitir el drop
    },
    []
  )

  const handleDrop = React.useCallback(
    (toIndex: number) => {
      if (dragIndex === null || dragIndex === toIndex) return

      setData(prev => {
        const updated = [...prev]
        const [moved] = updated.splice(dragIndex, 1)
        updated.splice(toIndex, 0, moved)
        return updated
      })

      setDragIndex(null)
    },
    [dragIndex]
  )

  // ---- Columnas con celdas editables ----
  const columns = React.useMemo<ColumnDef<Match>[]>(
    () => [
      {
        header: 'Equip',
        accessorKey: 'equipo',
        cell: ({ row, getValue }) => (
          <input
            className="w-full bg-transparent outline-none"
            value={(getValue() as string) ?? ''}
            onChange={e =>
              handleCellChange(row.index, 'equipo', e.target.value)
            }
          />
        ),
      },
      {
        header: 'Rival',
        accessorKey: 'rival',
        cell: ({ row, getValue }) => (
          <input
            className="w-full bg-transparent outline-none"
            value={(getValue() as string) ?? ''}
            onChange={e =>
              handleCellChange(row.index, 'rival', e.target.value)
            }
          />
        ),
      },
      {
        header: 'Data',
        accessorKey: 'data',
        cell: ({ row, getValue }) => (
          <input
            className="w-full bg-transparent outline-none"
            value={(getValue() as string) ?? ''}
            onChange={e =>
              handleCellChange(row.index, 'data', e.target.value)
            }
          />
        ),
      },
      {
        header: 'Horari',
        accessorKey: 'horari',
        cell: ({ row, getValue }) => (
          <input
            className="w-full bg-transparent outline-none"
            value={(getValue() as string) ?? ''}
            onChange={e =>
              handleCellChange(row.index, 'horari', e.target.value)
            }
          />
        ),
      },
      {
        header: 'Camp',
        accessorKey: 'camp',
        cell: ({ row, getValue }) => (
          <input
            className="w-full bg-transparent outline-none"
            value={(getValue() as string) ?? ''}
            onChange={e =>
              handleCellChange(row.index, 'camp', e.target.value)
            }
          />
        ),
      },
    ],
    [handleCellChange]
  )

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  return (
    <table className="mt-8 w-full table-auto border-collapse text-sm text-left">
      <thead>
        {table.getHeaderGroups().map(headerGroup => (
          <tr key={headerGroup.id}>
            {headerGroup.headers.map(header => (
              <th
                key={header.id}
                className="px-4 py-2 border-b font-semibold cursor-default select-none"
              >
                {flexRender(header.column.columnDef.header, header.getContext())}
              </th>
            ))}
          </tr>
        ))}
      </thead>

      <tbody>
        {table.getRowModel().rows.map(row => (
          <tr
            key={row.id}
            draggable
            onDragStart={() => handleDragStart(row.index)}
            onDragOver={handleDragOver}
            onDrop={() => handleDrop(row.index)}
            className="hover:bg-gray-100 dark:hover:bg-gray-700 cursor-move"
          >
            {row.getVisibleCells().map(cell => (
              <td key={cell.id} className="px-4 py-2 border-b">
                {flexRender(cell.column.columnDef.cell, cell.getContext())}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export default MatchesTable
