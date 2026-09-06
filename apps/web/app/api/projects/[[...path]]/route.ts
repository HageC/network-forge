import { NextRequest, NextResponse } from 'next/server';

type Context = { params: Promise<{ path?: string[] }> };
async function proxy(request: NextRequest, context: Context) {
  const { path = [] } = await context.params;
  if (path.some(segment => !/^[a-zA-Z0-9-]+$/.test(segment))) return NextResponse.json({ error: 'Invalid path' }, { status: 400 });
  const body = request.method === 'GET' || request.method === 'DELETE' ? undefined : await request.text();
  if (body && body.length > 1_000_000) return NextResponse.json({ error: 'Request too large' }, { status: 413 });
  try {
    const response = await fetch(`${process.env.NETWORKFORGE_API_URL ?? 'http://localhost:8080'}/api/v1/projects${path.length ? '/' + path.join('/') : ''}`, { method: request.method, headers: body ? { 'Content-Type': 'application/json' } : undefined, body, cache: 'no-store' });
    if (response.status === 204) return new NextResponse(null, { status: 204 });
    return new NextResponse(await response.text(), { status: response.status, headers: { 'Content-Type': 'application/json' } });
  } catch { return NextResponse.json({ error: 'Project API unavailable. Start the Go service.' }, { status: 503 }); }
}
export const GET = proxy;
export const POST = proxy;
export const PATCH = proxy;
export const DELETE = proxy;
