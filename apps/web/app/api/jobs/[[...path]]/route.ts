import { NextRequest, NextResponse } from "next/server";

type Context = { params: Promise<{ path?: string[] }> };

async function proxy(request: NextRequest, context: Context) {
  const { path = [] } = await context.params;
  if (path.length > 1 || path.some((part) => !/^[a-f0-9]{32}$/.test(part))) {
    return NextResponse.json({ error: "Invalid job ID" }, { status: 400 });
  }
  const body = request.method === "POST" ? await request.text() : undefined;
  if (body && body.length > 1_000_000) {
    return NextResponse.json({ error: "Request too large" }, { status: 413 });
  }
  try {
    const response = await fetch(
      `${process.env.NETWORKFORGE_API_URL ?? "http://localhost:8080"}/api/v1/jobs${path.length ? `/${path[0]}` : ""}`,
      {
        method: request.method,
        headers: body ? { "Content-Type": "application/json" } : undefined,
        body,
        cache: "no-store",
      },
    );
    return new NextResponse(await response.text(), {
      status: response.status,
      headers: { "Content-Type": "application/json" },
    });
  } catch {
    return NextResponse.json({ error: "Job API unavailable" }, { status: 503 });
  }
}

export const GET = proxy;
export const POST = proxy;
export const DELETE = proxy;
