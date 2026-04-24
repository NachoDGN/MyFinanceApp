import type { GenerateJsonParams, LLMTaskClient } from "./types";

export type PromptMessages = {
  systemPrompt: string;
  userPrompt: string;
};

export function runStructuredPromptTask<T>(
  client: LLMTaskClient,
  prompt: PromptMessages,
  options: Omit<GenerateJsonParams<T>, "systemPrompt" | "userPrompt">,
) {
  return client.generateJson({
    ...options,
    systemPrompt: prompt.systemPrompt,
    userPrompt: prompt.userPrompt,
  });
}
