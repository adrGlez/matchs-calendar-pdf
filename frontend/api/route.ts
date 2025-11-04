// import { NextRequest } from 'next/server';
// import { redirect } from 'next/navigation';

export async function getScrapedMatches(/* request: NextRequest */) {
  try {
    const res = await fetch('http://localhost:8000/fcf/scrape');
    const data = await res.json();
    console.log(data)
    return data;
  } catch (error) {
    console.error(error)
  }
}

// export async function POST(request: NextRequest) {
//   const body = await request.json();
//   return Response.json({ received: body });
// }

// export async function DELETE(request: NextRequest) {
//   return Response.json({ message: 'Resource deleted' });
// }

// export async function PATCH(request: NextRequest) {
//   const body = await request.json();
//   return Response.json({ message: 'Resource updated', data: body });
// }

// export async function OPTIONS(request: NextRequest) {
//   return new Response(null, {
//     status: 204,
//     headers: {
//       Allow: 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
//     },
//   });
// }
