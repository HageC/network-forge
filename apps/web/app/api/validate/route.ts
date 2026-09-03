import { NextRequest, NextResponse } from "next/server";

export async function POST(request: NextRequest) {
  const body = await request.text();
  if (body.length > 1_000_000)
    return NextResponse.json({ error: "Request too large" }, { status: 413 });
  try {
    const response = await fetch(
      `${process.env.NETWORKFORGE_API_URL ?? "http://localhost:8080"}/api/v1/validate`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
        cache: "no-store",
        signal: request.signal,
      },
    );
    return new NextResponse(await response.text(), {
      status: response.status,
      headers: { "Content-Type": "application/json" },
    });
  } catch {
    return NextResponse.json(
      { error: "Validation API unavailable" },
      { status: 503 },
    );
  }
}
