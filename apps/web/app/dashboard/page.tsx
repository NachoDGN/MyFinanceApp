import { DashboardView } from "../../components/dashboard-view";
import { getDashboardModel } from "../../lib/queries";

export default async function DashboardPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getDashboardModel(searchParams);
  return (
    <DashboardView
      pathname="/dashboard"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
      model={model}
    />
  );
}
