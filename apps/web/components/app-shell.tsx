import type { ReactNode } from "react";
import { redirect } from "next/navigation";

import { buildHref, type NavigationState } from "../lib/navigation";
import { ImportReviewModalHost } from "./import-review-modal-host";
import { TransactionSearchModalHost } from "./transaction-search-modal-host";

const primaryNav = [
  { href: "/", label: "Dashboard" },
  { href: "/spending", label: "Spending" },
  { href: "/income", label: "Income" },
  { href: "/investments", label: "Investments" },
  { href: "/accounts", label: "Accounts" },
];

const secondaryNav = [
  { href: "/transactions", label: "Transactions" },
  { href: "/categories", label: "Categories" },
  { href: "/imports", label: "Imports" },
  { href: "/templates", label: "Templates" },
  { href: "/prompts", label: "Prompts" },
  { href: "/rules", label: "Rules" },
  { href: "/insights", label: "Insights" },
  { href: "/settings", label: "Settings" },
];

const reservedNavigationParamNames = new Set([
  "scope",
  "currency",
  "period",
  "asOf",
  "start",
  "end",
]);

export function AppShell({
  children,
  pathname,
  state,
  scopeOptions,
  pageQueryParams = {},
}: {
  children: ReactNode;
  pathname: string;
  state: NavigationState;
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
  const supportsCustomPeriod =
    pathname === "/spending" ||
    pathname.startsWith("/spending/") ||
    pathname === "/income" ||
    pathname.startsWith("/income/");
  const shouldNormalizeReferenceDate =
    Boolean(state.referenceDate) &&
    Boolean(state.latestReferenceDate) &&
    state.referenceDate !== state.latestReferenceDate;

  if (shouldNormalizeReferenceDate) {
    redirect(
      buildHref(
        pathname,
        state,
        { referenceDate: state.latestReferenceDate },
        pageQueryParams,
      ),
    );
  }

  return (
    <>
      <header className="app-header">
        <div className="app-header-inner">
          <div className="app-header-main">
            <a className="brand-mark" href={buildHref("/", state, {})}>
              <span className="brand-logo" aria-hidden="true">
                <span />
              </span>
              <span className="brand-name">LedgerSpace</span>
            </a>

            <nav className="top-nav" aria-label="Primary navigation">
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

            <div className="nav-actions" aria-label="Account actions">
              <button className="nav-icon-button" type="button">
                <span className="nav-bell-icon" aria-hidden="true" />
                <span className="sr-only">Notifications</span>
              </button>
              <div className="nav-avatar" aria-label="Current user">
                ID
              </div>
            </div>
          </div>

          <nav className="secondary-nav" aria-label="Secondary navigation">
            {secondaryNav.map((item) => (
              <a
                key={item.href}
                href={buildHref(item.href, state, {})}
                className={isSecondaryNavActive(item.href) ? "active" : ""}
              >
                {item.label}
              </a>
            ))}
          </nav>
        </div>
      </header>

      <main className="page-shell app-main">
        <div className="filter-bar">
          <div className="filter-group segmented-filter scope-filter-group">
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

          <div className="filter-actions">
            <div className="filter-group segmented-filter period-filter-group">
              <a
                href={buildHref(
                  pathname,
                  state,
                  { period: "mtd" },
                  pageQueryParams,
                )}
                className={`filter-pill ${state.period === "mtd" ? "active" : ""}`}
              >
                MTD
              </a>
              <a
                href={buildHref(
                  pathname,
                  state,
                  { period: "last_month" },
                  pageQueryParams,
                )}
                className={`filter-pill ${state.period === "last_month" ? "active" : ""}`}
              >
                Last Month
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
                YTD
              </a>
              <a
                href={buildHref(
                  pathname,
                  state,
                  { period: "all" },
                  pageQueryParams,
                )}
                className={`filter-pill ${state.period === "all" ? "active" : ""}`}
              >
                ALL
              </a>
            </div>

            {supportsCustomPeriod ? (
              <form className="custom-period-form" action={pathname}>
                <input type="hidden" name="scope" value={state.scopeParam} />
                <input type="hidden" name="currency" value={state.currency} />
                <input type="hidden" name="period" value="custom" />
                {state.referenceDate ? (
                  <input
                    type="hidden"
                    name="asOf"
                    value={state.referenceDate}
                  />
                ) : null}
                {Object.entries(pageQueryParams)
                  .filter(
                    ([key, value]) =>
                      !reservedNavigationParamNames.has(key) &&
                      typeof value === "string" &&
                      value.trim() !== "",
                  )
                  .map(([key, value]) => (
                    <input key={key} type="hidden" name={key} value={value} />
                  ))}
                <input
                  aria-label="Custom start date"
                  className="custom-period-input"
                  name="start"
                  type="date"
                  defaultValue={state.start}
                  required
                />
                <input
                  aria-label="Custom end date"
                  className="custom-period-input"
                  name="end"
                  type="date"
                  defaultValue={state.end}
                  required
                />
                <button
                  className={`filter-pill custom-period-submit ${
                    state.period === "custom" ? "active" : ""
                  }`}
                  type="submit"
                >
                  Apply
                </button>
              </form>
            ) : null}

            <div className="filter-group segmented-filter currency-filter-group">
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

            <TransactionSearchModalHost state={state} />
          </div>
        </div>
        <ImportReviewModalHost />
        {children}
      </main>
    </>
  );
}
