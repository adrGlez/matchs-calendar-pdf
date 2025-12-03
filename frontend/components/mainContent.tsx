// import React from 'react'

import Link from "next/link"

const Header = () => {
  return (
    <div className='bg-[#032652]/60 p-12'>
      <p>Herramienta interna del CF Mollet UE</p>
      <h1 className="text-4xl font-semibold">Todos los partidos de la jornada en un solo click</h1>
      <p>Recogemos la información automáticamente (mediante scraping) y generamos la tabla editable con todos los partidos del club.</p>
      <Link href="./matches">
        <button className="bg-[#F5A623] rounded-lg p-2">Actualizar jornada ahora</button>
      </Link>
      <p className="text-xs">Ejecutaremos el scraping en tiempo real.</p>
      <p className="text-lg font-medium">Ver cómo funciona →</p>
      <img src="https://thumbs.dreamstime.com/b/plantilla-de-dise%C3%B1o-tabla-datos-simple-con-estilo-oscuro-multiprop%C3%B3sito-una-bonita-versi%C3%B3n-color-minimalista-esquinas-259522250.jpg" alt="tabla_img" />
    </div>
  )
}

export default Header
