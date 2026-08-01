import type * as React from "react";

/**
 * Home hero band above the catalog.
 * Premium/editorial treatment: deep navy→ocean gradient, faint topographic
 * contour arcs, an abstract cyan "saga path" with node dots, and a soft
 * radial glow. Pure inline SVG, zero raster, aria-hidden artwork.
 */
export function HomeHero() {
  return (
    <div className="relative flex h-[200px] w-full items-stretch overflow-hidden rounded-2xl border border-border sm:h-[230px] md:h-[260px]">
      {/* Dark artwork layer (full-bleed bg, text sits on top) */}
      <svg
        aria-hidden="true"
        className="absolute inset-0 h-full w-full"
        viewBox="0 0 1200 260"
        preserveAspectRatio="xMidYMid slice"
      >
        <defs>
          {/* Deep navy → primary, smooth dark diagonal */}
          <linearGradient id="hero-bg" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stopColor="#081F3A" />
            <stop offset="45%" stopColor="#0A2B4E" />
            <stop offset="100%" stopColor="#0060AF" />
          </linearGradient>

          {/* Soft depth highlight, upper-right */}
          <radialGradient id="hero-glow" cx="0.78" cy="0.22" r="0.55">
            <stop offset="0%" stopColor="#7FD4FF" stopOpacity="0.28" />
            <stop offset="55%" stopColor="#3D9BDE" stopOpacity="0.12" />
            <stop offset="100%" stopColor="#0060AF" stopOpacity="0" />
          </radialGradient>

          {/* Fade the contour field toward the left so text stays clean */}
          <linearGradient id="hero-contour-fade" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="#000000" stopOpacity="0" />
            <stop offset="38%" stopColor="#000000" stopOpacity="0" />
            <stop offset="60%" stopColor="#FFFFFF" stopOpacity="1" />
            <stop offset="100%" stopColor="#FFFFFF" stopOpacity="1" />
          </linearGradient>
          <mask id="hero-contour-mask">
            <rect width="1200" height="260" fill="url(#hero-contour-fade)" />
          </mask>
        </defs>

        {/* Base gradient */}
        <rect width="1200" height="260" fill="url(#hero-bg)" />

        {/* Topographic contour arcs sweeping the right half */}
        <g
          fill="none"
          stroke="#FFFFFF"
          strokeWidth="1"
          mask="url(#hero-contour-mask)"
        >
          <circle cx="1010" cy="40" r="70" strokeOpacity="0.10" />
          <circle cx="1010" cy="40" r="118" strokeOpacity="0.09" />
          <circle cx="1010" cy="40" r="168" strokeOpacity="0.08" />
          <circle cx="1010" cy="40" r="220" strokeOpacity="0.07" />
          <circle cx="1010" cy="40" r="274" strokeOpacity="0.06" />
          <circle cx="1010" cy="40" r="330" strokeOpacity="0.05" />
          <circle cx="1010" cy="40" r="388" strokeOpacity="0.045" />
          <circle cx="1010" cy="40" r="448" strokeOpacity="0.04" />
          {/* Second, offset contour family for a layered terrain feel */}
          <circle cx="700" cy="300" r="90" strokeOpacity="0.06" />
          <circle cx="700" cy="300" r="150" strokeOpacity="0.05" />
          <circle cx="700" cy="300" r="212" strokeOpacity="0.045" />
          <circle cx="700" cy="300" r="276" strokeOpacity="0.04" />
          <circle cx="700" cy="300" r="342" strokeOpacity="0.035" />
        </g>

        {/* Slow-drifting glow (static when reduced motion is preferred) */}
        <g className="hero-drift">
          <rect width="1200" height="260" fill="url(#hero-glow)" />
        </g>

        {/* Abstract saga path — thin cyan arc with node dots, right half */}
        <g fill="none">
          <path
            d="M560,178 C680,150 780,108 900,96 C985,88 1060,94 1130,66"
            stroke="#00A6E0"
            strokeOpacity="0.55"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeDasharray="1 7"
          />
          <path
            d="M560,178 C680,150 780,108 900,96 C985,88 1060,94 1130,66"
            stroke="#00A6E0"
            strokeOpacity="0.6"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeDasharray="34 200"
            strokeDashoffset="0"
          />

          {/* Node dots along the path */}
          <circle cx="655" cy="146" r="3.5" fill="#00A6E0" fillOpacity="0.9" />
          <circle cx="655" cy="146" r="7.5" stroke="#00A6E0" strokeOpacity="0.35" strokeWidth="1" />
          <circle cx="900" cy="96" r="3.5" fill="#7FD4FF" fillOpacity="0.95" />
          <circle cx="900" cy="96" r="8.5" stroke="#00A6E0" strokeOpacity="0.4" strokeWidth="1" />
          <circle cx="1130" cy="66" r="3.5" fill="#7FD4FF" fillOpacity="0.95" />
          <circle cx="1130" cy="66" r="7.5" stroke="#00A6E0" strokeOpacity="0.35" strokeWidth="1" />
        </g>

        {/* Hairline horizon accents, far left-below, barely-there */}
        <path
          d="M0,214 C180,206 340,206 520,212"
          stroke="#FFFFFF"
          strokeOpacity="0.05"
          strokeWidth="1"
          fill="none"
        />
        <path
          d="M0,232 C200,226 360,226 560,230"
          stroke="#FFFFFF"
          strokeOpacity="0.04"
          strokeWidth="1"
          fill="none"
        />
      </svg>

      {/* Wording block — above the artwork */}
      <div className="relative z-10 flex w-full flex-col justify-center px-8 md:px-12">
        <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-cyan-300/80">
          Ocean Retail Demo
        </p>
        <h1 className="mt-2.5 max-w-[16ch] text-2xl font-semibold tracking-tight text-white md:text-3xl">
          Every order tells a saga.
        </h1>
        <p className="mt-2.5 max-w-[46ch] text-sm text-white/60">
          Choreographed across cart, payment, warehouse, and doorstep — one
          resilient flow.
        </p>
      </div>
    </div>
  );
}
