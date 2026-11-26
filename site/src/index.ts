// Copyright 2026 Economico, LLC
// SPDX-License-Identifier: MIT

interface Env {
  ASSETS: Fetcher;
}

const SECURITY_HEADERS: Record<string, string> = {
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
  "Referrer-Policy": "strict-origin-when-cross-origin",
  "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Canonicalize www.cloudflake.dev -> https://cloudflake.dev
    if (url.hostname === "www.cloudflake.dev") {
      url.hostname = "cloudflake.dev";
      url.protocol = "https:";
      return Response.redirect(url.toString(), 301);
    }

    const assetResponse = await env.ASSETS.fetch(request);
    const headers = new Headers(assetResponse.headers);
    for (const [key, value] of Object.entries(SECURITY_HEADERS)) {
      headers.set(key, value);
    }

    // Serve /skill.md and its /references/*.md docs as raw UTF-8 markdown with permissive CORS
    if (
      url.pathname === "/skill.md" ||
      (url.pathname.startsWith("/references/") && url.pathname.endsWith(".md"))
    ) {
      headers.set("Content-Type", "text/markdown; charset=utf-8");
      headers.set("Access-Control-Allow-Origin", "*");
      headers.set("Cache-Control", "public, max-age=300");
    }

    return new Response(assetResponse.body, {
      status: assetResponse.status,
      statusText: assetResponse.statusText,
      headers,
    });
  },
};
