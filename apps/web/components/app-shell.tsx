import type { ReactNode } from "react";

import { buildHref } from "../lib/queries";

const primaryNav = [
  { href: "/", label: "Dashboard" },
  { href: "/spending", label: "Spending" },
  { href: "/income", label: "Income" },
  { href: "/investments", label: "Investments" },
  { href: "/accounts", label: "Accounts" },
];

const secondaryNav = [
  { href: "/transactions", label: "Transactions" },
  { href: "/imports", label: "Imports" },
  { href: "/templates", label: "Templates" },
  { href: "/rules", label: "Rules" },
  { href: "/insights", label: "Insights" },
  { href: "/settings", label: "Settings" },
];

export function AppShell({
  children,
  pathname,
  state,
  scopeOptions,
}: {
  children: ReactNode;
  pathname: string;
  state: {
    scopeParam: string;
    currency: string;
    period: string;
    referenceDate?: string;
    start?: string;
    end?: string;
  };
  scopeOptions: Array<{ value: string; label: string }>;
}) {
  return (
    <div className="page-shell">
      <nav className="top-nav">
        <div className="nav-logo" />
        {primaryNav.map((item) => (
          <a
            key={item.href}
            href={buildHref(item.href, state, {})}
            className={`nav-item ${pathname === item.href ? "active" : ""}`}
          >
            {item.label}
          </a>
        ))}
      </nav>

      <div className="secondary-nav">
        {secondaryNav.map((item) => (
          <a
            key={item.href}
            href={buildHref(item.href, state, {})}
            className={pathname === item.href ? "active" : ""}
          >
            {item.label}
          </a>
        ))}
      </div>

      <div className="filter-bar">
        <div className="filter-group">
          {scopeOptions.slice(0, 4).map((option) => (
            <a
              key={option.value}
              href={buildHref(pathname, state, { scopeParam: option.value })}
              className={`filter-pill ${state.scopeParam === option.value ? "active" : ""}`}
            >
              {option.label}
            </a>
          ))}
        </div>
        <div className="filter-group">
          <a
            href={buildHref(pathname, state, { period: "mtd" })}
            className={`filter-pill ${state.period === "mtd" ? "active" : ""}`}
          >
            Month to Date
          </a>
          <a
            href={buildHref(pathname, state, { period: "ytd" })}
            className={`filter-pill ${state.period === "ytd" ? "active" : ""}`}
          >
            Year to Date
          </a>
          <a
            href={buildHref(pathname, state, { currency: "EUR" })}
            className={`filter-pill ${state.currency === "EUR" ? "active" : ""}`}
          >
            EUR
          </a>
          <a
            href={buildHref(pathname, state, { currency: "USD" })}
            className={`filter-pill ${state.currency === "USD" ? "active" : ""}`}
          >
            USD
          </a>
        </div>
      </div>
      {children}
    </div>
  );
}
