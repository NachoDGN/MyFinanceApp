import { AppShell } from "../../components/app-shell";
import { PromptWorkbench } from "../../components/prompt-workbench";
import { getPromptsModel } from "../../lib/queries";

export default async function PromptsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getPromptsModel(searchParams);

  return (
    <AppShell
      pathname="/prompts"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid">
        <div className="page-header">
          <div>
            <h1 className="page-title">Prompts</h1>
            <p className="page-subtitle">
              Review the exact prompt templates sent to each LLM task and edit
              the non-variable wording for this workspace, including the
              learned examples kept for future review retries.
            </p>
          </div>
        </div>

        <div style={{ gridColumn: "1 / -1" }}>
          <PromptWorkbench
            initialProfiles={model.promptProfiles}
            accounts={model.dataset.accounts}
            initialLearnedReviewExamples={model.learnedReviewExamples}
          />
        </div>
      </div>
    </AppShell>
  );
}
