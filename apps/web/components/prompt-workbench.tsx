"use client";

import { useState, useTransition } from "react";

import type { PromptProfileModel } from "@myfinance/db";
import { buildPromptProfilePreview } from "@myfinance/llm";

import { updatePromptProfileAction } from "../app/actions";

type PreviewMode = "system" | "user";

function getPreviewModeForSection(sectionId: string): PreviewMode {
  return sectionId === "system_prompt" ? "system" : "user";
}

function getSectionTone(sectionId: string) {
  switch (sectionId) {
    case "system_prompt":
      return "System";
    case "user_prompt_template":
      return "User";
    case "review_examples_wrapper":
      return "Wrapper";
    case "review_example_template":
      return "Example";
    case "review_context_template":
      return "Review";
    case "sheet_preview_template":
      return "Sheet";
    default:
      return "Section";
  }
}

function countNonEmptyLines(value: string) {
  return value.split("\n").filter((line) => line.trim().length > 0).length;
}

function buildSectionOverrides(profile: PromptProfileModel) {
  return Object.fromEntries(
    profile.editableSections.map((section) => [section.id, section.value]),
  );
}

export function PromptWorkbench({
  initialProfiles,
}: {
  initialProfiles: PromptProfileModel[];
}) {
  const [profiles, setProfiles] = useState<PromptProfileModel[]>(initialProfiles);
  const [selectedPromptId, setSelectedPromptId] = useState<
    PromptProfileModel["id"] | ""
  >(initialProfiles[0]?.id ?? "");
  const [selectedSectionId, setSelectedSectionId] = useState<string>(
    initialProfiles[0]?.editableSections[0]?.id ?? "",
  );
  const [previewMode, setPreviewMode] = useState<PreviewMode>(
    getPreviewModeForSection(initialProfiles[0]?.editableSections[0]?.id ?? ""),
  );
  const [feedback, setFeedback] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const selectedProfile =
    profiles.find((profile) => profile.id === selectedPromptId) ?? profiles[0];
  const selectedSection =
    selectedProfile?.editableSections.find(
      (section) => section.id === selectedSectionId,
    ) ?? selectedProfile?.editableSections[0];

  if (!selectedProfile || !selectedSection) {
    return (
      <div className="status-note">
        No prompt profiles are available for the current workspace.
      </div>
    );
  }

  const livePreview = buildPromptProfilePreview(
    selectedProfile.id,
    buildSectionOverrides(selectedProfile),
  );
  const activeProfileId = selectedProfile.id;
  const activeSectionIndex = selectedProfile.editableSections.findIndex(
    (section) => section.id === selectedSection.id,
  );
  const totalRequiredPlaceholders = selectedProfile.editableSections.reduce(
    (total, section) => total + section.requiredPlaceholders.length,
    0,
  );

  function updateSectionValue(sectionId: string, value: string) {
    setProfiles((current) =>
      current.map((profile) =>
        profile.id !== activeProfileId
          ? profile
          : {
              ...profile,
              editableSections: profile.editableSections.map((section) =>
                section.id === sectionId ? { ...section, value } : section,
              ),
            },
      ),
    );
  }

  function handlePromptChange(nextPromptId: PromptProfileModel["id"]) {
    const nextProfile =
      profiles.find((profile) => profile.id === nextPromptId) ?? profiles[0];

    setSelectedPromptId(nextPromptId);
    setSelectedSectionId(nextProfile?.editableSections[0]?.id ?? "");
    setPreviewMode(
      getPreviewModeForSection(nextProfile?.editableSections[0]?.id ?? ""),
    );
    setFeedback(null);
  }

  function handleSectionChange(sectionId: string) {
    setSelectedSectionId(sectionId);
    setPreviewMode(getPreviewModeForSection(sectionId));
    setFeedback(null);
  }

  function handleSave() {
    startTransition(async () => {
      setFeedback(null);
      try {
        const formData = new FormData();
        formData.set("promptId", selectedProfile.id);
        formData.set(
          "sectionsJson",
          JSON.stringify(buildSectionOverrides(selectedProfile)),
        );

        const result = await updatePromptProfileAction(formData);
        if (!result.profile) {
          throw new Error(`Prompt profile ${selectedProfile.id} was not returned.`);
        }

        setProfiles((current) =>
          current.map((profile) =>
            profile.id === result.profile?.id ? result.profile : profile,
          ),
        );
        setFeedback(result.message);
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Prompt update failed.",
        );
      }
    });
  }

  return (
    <div className="prompt-workbench">
      <section className="section-card prompt-profile-card">
        <div className="section-header">
          <div>
            <span className="label-sm">Prompt Picker</span>
            <h2 className="section-title">Choose the worker you want to tune</h2>
          </div>
          <span className="pill">{selectedProfile.modelName}</span>
        </div>
        <div className="section-card-body prompt-profile-body">
          <div className="prompt-profile-grid">
            <label className="input-label">
              Prompt / model
              <select
                className="input-select"
                value={selectedPromptId}
                onChange={(event) =>
                  handlePromptChange(
                    event.target.value as PromptProfileModel["id"],
                  )
                }
              >
                {profiles.map((profile) => (
                  <option key={profile.id} value={profile.id}>
                    {profile.title} · {profile.modelName}
                  </option>
                ))}
              </select>
            </label>

            <div className="prompt-profile-summary">
              <p className="muted">{selectedProfile.description}</p>
              <div className="prompt-summary-pills">
                <span className="pill">
                  {selectedProfile.editableSections.length} editable section
                  {selectedProfile.editableSections.length === 1 ? "" : "s"}
                </span>
                <span className="pill">
                  {totalRequiredPlaceholders} locked placeholder
                  {totalRequiredPlaceholders === 1 ? "" : "s"}
                </span>
                <span className="pill">{selectedProfile.id}</span>
              </div>
              <p className="muted">
                Edit one building block at a time. Placeholder tokens stay
                fixed, and the assembled preview updates live before you save.
              </p>
            </div>
          </div>
        </div>
      </section>

      <div className="prompt-workbench-layout">
        <aside className="section-card prompt-sections-card">
          <div className="section-header">
            <div>
              <span className="label-sm">Editable Sections</span>
              <h2 className="section-title">Prompt Structure</h2>
            </div>
            <span className="pill">
              {activeSectionIndex + 1} / {selectedProfile.editableSections.length}
            </span>
          </div>
          <div className="section-card-body prompt-sections-list">
            {selectedProfile.editableSections.map((section, index) => {
              const isActive = section.id === selectedSection.id;

              return (
                <button
                  key={section.id}
                  className={`prompt-section-button${isActive ? " active" : ""}`}
                  type="button"
                  onClick={() => handleSectionChange(section.id)}
                >
                  <div className="prompt-section-button-head">
                    <span className="prompt-section-index">
                      {String(index + 1).padStart(2, "0")}
                    </span>
                    <span className="pill">{getSectionTone(section.id)}</span>
                  </div>
                  <div className="prompt-section-label">{section.label}</div>
                  <div className="prompt-section-copy">{section.description}</div>
                  <div className="prompt-section-meta">
                    {section.requiredPlaceholders.length > 0
                      ? `${section.requiredPlaceholders.length} locked placeholder${
                          section.requiredPlaceholders.length === 1 ? "" : "s"
                        }`
                      : "No locked placeholders"}
                  </div>
                </button>
              );
            })}
          </div>
        </aside>

        <div className="prompt-content-grid">
          <section className="section-card prompt-editor-card">
            <div className="section-header">
              <div>
                <span className="label-sm">
                  Editing section {activeSectionIndex + 1}
                </span>
                <h2 className="section-title">{selectedSection.label}</h2>
              </div>
              <div className="prompt-editor-meta">
                <span className="pill">{getSectionTone(selectedSection.id)}</span>
                <span className="pill">
                  {countNonEmptyLines(selectedSection.value)} line
                  {countNonEmptyLines(selectedSection.value) === 1 ? "" : "s"}
                </span>
              </div>
            </div>
            <div className="section-card-body prompt-editor-body">
              <p className="muted prompt-editor-description">
                {selectedSection.description}
              </p>

              <label className="input-label">
                Editable copy
                <textarea
                  className="input-textarea prompt-editor-textarea"
                  rows={Math.min(
                    20,
                    Math.max(10, selectedSection.value.split("\n").length + 2),
                  )}
                  value={selectedSection.value}
                  onChange={(event) =>
                    updateSectionValue(selectedSection.id, event.target.value)
                  }
                />
              </label>

              {selectedSection.requiredPlaceholders.length > 0 ? (
                <details
                  key={selectedSection.id}
                  className="prompt-placeholder-details"
                  open={selectedSection.requiredPlaceholders.length <= 6}
                >
                  <summary className="prompt-placeholder-summary">
                    Locked placeholders ({selectedSection.requiredPlaceholders.length})
                  </summary>
                  <p className="muted">
                    These tokens must stay exactly as shown or validation will
                    reject the save.
                  </p>
                  <div className="prompt-placeholder-list">
                    {selectedSection.requiredPlaceholders.map((placeholder) => (
                      <span key={placeholder} className="pill">
                        {`{{${placeholder}}}`}
                      </span>
                    ))}
                  </div>
                </details>
              ) : (
                <div className="builder-panel">
                  <span className="label-sm">No locked placeholders</span>
                  <p className="builder-copy">
                    This section is pure instructional copy, so you can rewrite
                    it without preserving template tokens.
                  </p>
                </div>
              )}

              <div className="inline-actions">
                <button
                  className="btn-pill"
                  type="button"
                  disabled={isPending}
                  onClick={handleSave}
                >
                  {isPending ? "Saving..." : "Save Prompt"}
                </button>
                <span className="muted">
                  Saving updates the stored template used for future background
                  runs in this workspace.
                </span>
              </div>
              {feedback ? <div className="status-note">{feedback}</div> : null}
            </div>
          </section>

          <aside className="section-card prompt-preview-card">
            <div className="section-header">
              <div>
                <span className="label-sm">Live Preview</span>
                <h2 className="section-title">Assembled Output</h2>
              </div>
              <span className="pill">Literal placeholders</span>
            </div>
            <div className="section-card-body prompt-preview-body">
              <div className="prompt-preview-toggle" role="tablist" aria-label="Preview mode">
                <button
                  className={previewMode === "system" ? "active" : ""}
                  type="button"
                  onClick={() => setPreviewMode("system")}
                >
                  System prompt
                </button>
                <button
                  className={previewMode === "user" ? "active" : ""}
                  type="button"
                  onClick={() => setPreviewMode("user")}
                >
                  User prompt
                </button>
              </div>
              <p className="muted">
                The preview updates as you type. Save persists these edits for
                future LLM calls.
              </p>
              <pre className="prompt-preview-code">
                {previewMode === "system"
                  ? livePreview.systemPrompt
                  : livePreview.userPrompt}
              </pre>
            </div>
          </aside>
        </div>
      </div>
    </div>
  );
}
