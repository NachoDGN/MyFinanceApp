import type { ReactNode } from "react";

import { buildHref } from "../lib/queries";
import { ImportReviewModalHost } from "./import-review-modal-host";

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
  { href: "/prompts", label: "Prompts" },
  { href: "/rules", label: "Rules" },
  { href: "/insights", label: "Insights" },
  { href: "/settings", label: "Settings" },
];

export function AppShell({
  children,
  pathname,
  state,
  scopeOptions,
  pageQueryParams = {},
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
  pageQueryParams?: Record<string, string | undefined>;
}) {
  const entityScopeOptions = scopeOptions.filter(
    (option) =>
      option.value === "consolidated" || !option.value.startsWith("account:"),
  );
  const isStatementRoute = pathname.startsWith("/transactions/statements/");
  const isPrimaryNavActive = (href: string) =>
    href === "/"
      ? pathname === "/" || pathname === "/dashboard"
      : pathname === href || pathname.startsWith(`${href}/`);
  const isSecondaryNavActive = (href: string) =>
    pathname === href ||
    pathname.startsWith(`${href}/`) ||
    (isStatementRoute && href === "/transactions");

  return (
    <div className="page-shell">
      <nav className="top-nav">
        <div className="nav-logo" />
        {primaryNav.map((item) => (
          <a
            key={item.href}
            href={buildHref(item.href, state, {})}
            className={`nav-item ${isPrimaryNavActive(item.href) ? "active" : ""}`}
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
            className={isSecondaryNavActive(item.href) ? "active" : ""}
          >
            {item.label}
          </a>
        ))}
      </div>

      <div className="filter-bar">
        <div className="filter-group">
          {entityScopeOptions.map((option) => (
            <a
              key={option.value}
              href={buildHref(
                pathname,
                state,
                { scopeParam: option.value },
                pageQueryParams,
              )}
              className={`filter-pill ${state.scopeParam === option.value ? "active" : ""}`}
            >
              {option.label}
            </a>
          ))}
        </div>
        <div className="filter-group">
          <a
            href={buildHref(
              pathname,
              state,
              { period: "mtd" },
              pageQueryParams,
            )}
            className={`filter-pill ${state.period === "mtd" ? "active" : ""}`}
          >
            Month to Date
          </a>
          <a
            href={buildHref(
              pathname,
              state,
              { period: "ytd" },
              pageQueryParams,
            )}
            className={`filter-pill ${state.period === "ytd" ? "active" : ""}`}
          >
            Year to Date
          </a>
          <a
            href={buildHref(
              pathname,
              state,
              { currency: "EUR" },
              pageQueryParams,
            )}
            className={`filter-pill ${state.currency === "EUR" ? "active" : ""}`}
          >
            EUR
          </a>
          <a
            href={buildHref(
              pathname,
              state,
              { currency: "USD" },
              pageQueryParams,
            )}
            className={`filter-pill ${state.currency === "USD" ? "active" : ""}`}
          >
            USD
          </a>
        </div>
      </div>
      <ImportReviewModalHost />
      {children}
    </div>
  );
}
