"use client";

import { useEffect, useState } from "react";

export function useClientSearchParams() {
  const [searchParams, setSearchParams] = useState(
    () => new URLSearchParams(typeof window === "undefined" ? "" : window.location.search),
  );

  useEffect(() => {
    setSearchParams(new URLSearchParams(window.location.search));
  }, []);

  return searchParams;
}
