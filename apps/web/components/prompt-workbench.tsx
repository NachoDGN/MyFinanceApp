"use client";

import { useState, useTransition } from "react";

import type { PromptProfileModel } from "@myfinance/db";

import { updatePromptProfileAction } from "../app/actions";

export function PromptWorkbench({
  initialProfiles,
}: {
  initialProfiles: PromptProfileModel[];
}) {
  const [profiles, setProfiles] = useState<PromptProfileModel[]>(initialProfiles);
  const [selectedPromptId, setSelectedPromptId] = useState<
    PromptProfileModel["id"] | ""
  >(
    initialProfiles[0]?.id ?? "",
  );
  const [feedback, setFeedback] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const selectedProfile =
    profiles.find((profile) => profile.id === selectedPromptId) ?? profiles[0];

  function updateSectionValue(sectionId: string, value: string) {
    setProfiles((current) =>
      current.map((profile) =>
        profile.id !== selectedPromptId
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

  function handleSave() {
    if (!selectedProfile) {
      return;
    }

    startTransition(async () => {
      setFeedback(null);
      try {
        const formData = new FormData();
        formData.set("promptId", selectedProfile.id);
        formData.set(
          "sectionsJson",
          JSON.stringify(
            Object.fromEntries(
              selectedProfile.editableSections.map((section) => [
                section.id,
                section.value,
              ]),
            ),
          ),
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

  if (!selectedProfile) {
    return (
      <div className="status-note">
        No prompt profiles are available for the current workspace.
      </div>
    );
  }

  return (
    <div
      style={{
        display: "grid",
        gap: 20,
        gridTemplateColumns: "minmax(0, 0.95fr) minmax(0, 1.05fr)",
      }}
    >
      <section className="section-card span-6">
        <div className="section-header">
          <div>
            <span className="label-sm">Prompt Picker</span>
            <h2 className="section-title">Editable Sections</h2>
          </div>
          <span className="pill">{selectedProfile.modelName}</span>
        </div>
        <div className="section-card-body" style={{ display: "grid", gap: 16 }}>
          <label style={{ display: "grid", gap: 6 }}>
            <span className="label-sm">Prompt / Model</span>
            <select
              className="input-select"
              value={selectedPromptId}
              onChange={(event) => {
                setSelectedPromptId(event.target.value as PromptProfileModel["id"]);
                setFeedback(null);
              }}
            >
              {profiles.map((profile) => (
                <option key={profile.id} value={profile.id}>
                  {profile.title} · {profile.modelName}
                </option>
              ))}
            </select>
          </label>

          <div className="legend-list">
            <span className="pill">{selectedProfile.title}</span>
            <span className="pill">{selectedProfile.id}</span>
            <p className="muted">{selectedProfile.description}</p>
            <p className="muted">
              Keep every placeholder token such as <code>{"{{transaction_date}}"}</code>.
              The editor validates those tokens on save so only the non-variable wording changes.
            </p>
          </div>

          {selectedProfile.editableSections.map((section) => (
            <label key={section.id} style={{ display: "grid", gap: 8 }}>
              <div>
                <div className="label-sm">{section.label}</div>
                <div className="muted" style={{ fontSize: 13, lineHeight: 1.5 }}>
                  {section.description}
                </div>
              </div>
              <textarea
                className="input-textarea"
                rows={Math.min(
                  16,
                  Math.max(4, section.value.split("\n").length + 1),
                )}
                value={section.value}
                onChange={(event) =>
                  updateSectionValue(section.id, event.target.value)
                }
              />
              {section.requiredPlaceholders.length > 0 ? (
                <div className="legend-list">
                  {section.requiredPlaceholders.map((placeholder) => (
                    <span key={placeholder} className="pill">
                      {`{{${placeholder}}}`}
                    </span>
                  ))}
                </div>
              ) : null}
            </label>
          ))}

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
              Saved changes affect future LLM calls for this workspace.
            </span>
          </div>
          {feedback ? <div className="status-note">{feedback}</div> : null}
        </div>
      </section>

      <section className="section-card span-6">
        <div className="section-header">
          <div>
            <span className="label-sm">Saved Preview</span>
            <h2 className="section-title">Prompt Output</h2>
          </div>
          <span className="pill">Placeholders shown literally</span>
        </div>
        <div className="section-card-body" style={{ display: "grid", gap: 16 }}>
          <div style={{ display: "grid", gap: 8 }}>
            <div className="label-sm">System Prompt</div>
            <pre
              style={{
                margin: 0,
                padding: 16,
                borderRadius: 18,
                background: "rgba(12, 12, 12, 0.05)",
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                fontSize: 13,
                lineHeight: 1.6,
              }}
            >
              {selectedProfile.preview.systemPrompt}
            </pre>
          </div>

          <div style={{ display: "grid", gap: 8 }}>
            <div className="label-sm">User Prompt</div>
            <pre
              style={{
                margin: 0,
                padding: 16,
                borderRadius: 18,
                background: "rgba(12, 12, 12, 0.05)",
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                fontSize: 13,
                lineHeight: 1.6,
              }}
            >
              {selectedProfile.preview.userPrompt}
            </pre>
          </div>
        </div>
      </section>
    </div>
  );
}
