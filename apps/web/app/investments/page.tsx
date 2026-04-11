import { resolveInvestmentsPageModel } from "../../lib/investments-page";
import { InvestmentsPageView } from "../../components/investments-page-view";

export default async function InvestmentsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await resolveInvestmentsPageModel(searchParams);
  return <InvestmentsPageView model={model} />;
}
