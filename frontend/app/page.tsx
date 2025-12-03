'use client'

// import Image from "next/image";
// import { useState } from "react";
// import { getScrapedMatches } from "@/api/route";
// import Link from "next/link";
import Header from "@/components/header";
import MainContent from "@/components/mainContent";

export default function Page() {

  return (
    <div className="font-sans">
      <main className="flex min-h-screen w-full flex-col">
        <Header />
        <MainContent />

        {/* <Image
          className="dark:invert"
          src="/next.svg"
          alt="Next.js logo"
          width={100}
          height={20}
          priority
        /> */}
          {/* <Link
            className="flex h-12 w-full items-center justify-center gap-2 rounded-full bg-foreground px-5 text-background transition-colors hover:bg-[#383838] dark:hover:bg-[#ccc] md:w-[158px]"
            href="./matches"
            rel="noopener noreferrer"
          >
            <Image
              className="dark:invert"
              src="/vercel.svg"
              alt="Vercel logomark"
              width={16}
              height={16}
            />
            Matches list
          </Link> */}
      </main>
    </div>
  );
}
