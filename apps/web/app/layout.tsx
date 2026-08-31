import type { Metadata } from 'next';
import '@xyflow/react/dist/style.css';
import './styles.css';

export const metadata: Metadata = { title: 'NetworkForge', description: 'Distributed architecture simulator' };
export default function Layout({ children }: Readonly<{ children: React.ReactNode }>) { return <html lang="en"><body>{children}</body></html>; }
