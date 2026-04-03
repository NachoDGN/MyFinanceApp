import { DashboardView } from "../components/dashboard-view";
import { getDashboardModel } from "../lib/queries";

export default async function HomePage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getDashboardModel(searchParams);
  return (
    <DashboardView
      pathname="/"
      scopeOptions={model.scopeOptions}
      state={{
        scopeParam: model.scopeParam,
        currency: model.currency,
        period: model.period.preset,
      }}
      model={model}
    />
  );
}
