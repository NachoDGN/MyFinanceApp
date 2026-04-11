type EnvOverrides = Record<string, string | undefined>;

export function jsonResponse(
  body: unknown,
  init: ResponseInit = {
    status: 200,
    headers: { "Content-Type": "application/json" },
  },
) {
  return new Response(JSON.stringify(body), init);
}

export function readRequestUrl(input: string | URL | Request) {
  if (typeof input === "string") {
    return new URL(input);
  }
  if (input instanceof URL) {
    return input;
  }
  return new URL(input.url);
}

export async function withRuntimeOverrides<T>(
  options: {
    env?: EnvOverrides;
    fetch?: typeof globalThis.fetch;
  },
  runner: () => Promise<T> | T,
): Promise<T> {
  const previousEnv = new Map<string, string | undefined>();
  const envEntries = Object.entries(options.env ?? {});

  for (const [key, value] of envEntries) {
    previousEnv.set(key, process.env[key]);
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }

  const previousFetch = globalThis.fetch;
  if (options.fetch) {
    globalThis.fetch = options.fetch;
  }

  try {
    return await runner();
  } finally {
    if (options.fetch) {
      globalThis.fetch = previousFetch;
    }
    for (const [key, value] of previousEnv) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
}
