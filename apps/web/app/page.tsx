"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  type Connection,
  type Edge,
  type EdgeChange,
  type Node,
  type NodeChange,
} from "@xyflow/react";
import {
  Activity,
  ArrowRight,
  CornerUpLeft,
  CornerUpRight,
  Box,
  CircleHelp,
  Database,
  Gauge,
  Play,
  Plus,
  RotateCcw,
  Save,
  Server,
  Workflow,
} from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type Config = {
  kind: string;
  serviceTimeUs: number;
  concurrency: number;
  queueCapacity: number;
  connectionLimit: number;
  transactionLockUs?: number;
  brokerCapacity?: number;
  maxDeliveries?: number;
  redeliveryDelayUs?: number;
  autoscaleMaxConcurrency: number;
  autoscaleQueueThreshold: number;
  autoscaleDelayUs: number;
  autoscaleIdleUs: number;
  rateLimitRps: number;
  hitRate: number;
  hourlyCost: number;
};
type SimNode = Node<Config>;
type Failure = {
  id: string;
  kind:
    "node_outage" | "edge_partition" | "node_degradation" | "edge_loss_spike";
  targetId: string;
  startUs: number;
  durationUs: number;
  serviceMultiplier?: number;
  lossProbability?: number;
};
type TraceEvent = { timeUs: number; kind: string; componentId?: string };
type Trace = {
  id: string;
  requestIndex: number;
  startUs: number;
  endUs: number;
  latencyUs: number;
  status: string;
  events: TraceEvent[];
  truncated: boolean;
};
type Result = {
  generated: number;
  completed: number;
  failed: number;
  timedOut: number;
  meanLatencyUs: number;
  p50Us: number;
  p95Us: number;
  p99Us: number;
  nodes: Record<
    string,
    {
      completed: number;
      peakQueue: number;
      utilization: number;
      rateLimited: number;
      cacheHits: number;
      cacheMisses: number;
      failureDrops: number;
      outageUs: number;
      queueRejected: number;
      queueWaitObservations: number;
      meanQueueWaitUs: number;
      p95QueueWaitUs: number;
      connectionUtilization: number;
      connectionWaits: number;
      connectionRejected: number;
      lockWaits: number;
      totalLockWaitUs: number;
      brokerRejected: number;
      peakBrokerDepth: number;
      scaleUps: number;
      scaleDowns: number;
      peakConcurrency: number;
    }
  >;
  eventsProcessed: number;
  failuresTriggered: number;
  edgeDrops: Record<string, number>;
  retries: number;
  applicationRetries: number;
  applicationRetryExhausted: number;
  circuitOpens: number;
  circuitRejected: number;
  traces?: Trace[];
  edgeAttempts: Record<string, number>;
  configuredNodeCostPerHour: number;
  estimatedScenarioCost: number;
  estimatedHourlyCost: number;
  costPerSuccessfulRequest: number | null;
  asyncEnqueued: number;
  backgroundCompleted: number;
  backgroundFailed: number;
  backgroundTimedOut: number;
  backgroundRedeliveries: number;
  backgroundDeadLetters: number;
  findings: Array<{
    id: string;
    title: string;
    componentId?: string;
    severity: string;
    confidence: string;
    score: number;
    evidence: Array<{ metric: string; value: number; unit: string }>;
    recommendation: string;
    expectedImpact: string;
    tradeoffs: string;
  }>;
};
type ProjectSummary = { id: string; name: string; version: number };
type CalibrationPoint = { throughputRps: number; errorRate: number; p95Us: number; score: number };
type CalibrationResult = { baseline: CalibrationPoint; calibrated: CalibrationPoint; serviceTimeMultiplier: number; candidatesEvaluated: number };
type Project = ProjectSummary & {
  architecture: {
    nodes: Array<Config & { id: string; positionX: number; positionY: number }>;
    edges: Array<{
      id: string;
      source: string;
      target: string;
      latencyUs: number;
      lossProbability: number;
      maxRetries: number;
      retryBackoffUs: number;
      circuitFailureThreshold: number;
      circuitOpenUs: number;
      costPerMillionAttempts: number;
    }>;
    workload: {
      source: string;
      requestsPerSecond: number;
      durationSeconds: number;
      seed: number;
      maxApplicationRetries: number;
      applicationRetryBackoffUs: number;
    };
    failures: Failure[];
  };
};
type SavedRun = {
  id: number;
  version: number;
  createdAt: string;
  result: Result;
};
type ExperimentResult = {
  summary: {
    repetitions: number;
    completedLatencyRuns: number;
    meanP95Us: number;
    stddevP95Us: number;
    outcomeP05P95Us: number;
    outcomeP95P95Us: number;
    meanP95ConfidenceLowUs: number;
    meanP95ConfidenceHighUs: number;
    meanThroughputRps: number;
    meanErrorRate: number;
    sloViolationProbability: number;
    meanEstimatedScenarioCost: number;
    meanEstimatedHourlyCost: number;
    costPerSuccessfulRequest: number | null;
  };
  runs: Array<{
    seed: number;
    completed: number;
    p95Us: number;
    errorRate: number;
    throughputRps: number;
    sloViolated: boolean;
    estimatedScenarioCost: number;
    estimatedHourlyCost: number;
  }>;
};
type ComparisonResult = {
  objective: string;
  winner: "baseline" | "candidate" | "tie" | "inconclusive" | "indeterminate";
  objectiveDelta: number;
  objectiveDeltaConfidenceLow: number;
  objectiveDeltaConfidenceHigh: number;
  comparedPairs: number;
  baseline: ExperimentResult;
  candidate: ExperimentResult;
};
type ActiveJob = {
  id: string;
  kind: "simulation" | "project_run" | "experiment" | "comparison";
  projectId?: string;
};
type JobStatus = {
  id: string;
  kind: ActiveJob["kind"];
  status: "queued" | "running" | "succeeded" | "failed" | "cancelled";
  progressDone: number;
  progressTotal: number;
  result?: unknown;
  error?: string;
};
const initialNodes: SimNode[] = [
  {
    id: "client",
    position: { x: 70, y: 210 },
    data: {
      kind: "client",
      serviceTimeUs: 0,
      concurrency: 100,
      queueCapacity: 0,
      connectionLimit: 0,
      autoscaleMaxConcurrency: 0,
      autoscaleQueueThreshold: 0,
      autoscaleDelayUs: 0,
      autoscaleIdleUs: 0,
      rateLimitRps: 0,
      hitRate: 0,
      hourlyCost: 0,
    },
    type: "default",
  },
  {
    id: "gateway",
    position: { x: 340, y: 210 },
    data: {
      kind: "gateway",
      serviceTimeUs: 2000,
      concurrency: 100,
      queueCapacity: 100,
      connectionLimit: 0,
      autoscaleMaxConcurrency: 0,
      autoscaleQueueThreshold: 0,
      autoscaleDelayUs: 0,
      autoscaleIdleUs: 0,
      rateLimitRps: 0,
      hitRate: 0,
      hourlyCost: 0.08,
    },
    type: "default",
  },
  {
    id: "service",
    position: { x: 610, y: 210 },
    data: {
      kind: "service",
      serviceTimeUs: 35000,
      concurrency: 8,
      queueCapacity: 100,
      connectionLimit: 0,
      autoscaleMaxConcurrency: 0,
      autoscaleQueueThreshold: 0,
      autoscaleDelayUs: 0,
      autoscaleIdleUs: 0,
      rateLimitRps: 0,
      hitRate: 0,
      hourlyCost: 0.25,
    },
    type: "default",
  },
  {
    id: "database",
    position: { x: 880, y: 210 },
    data: {
      kind: "database",
      serviceTimeUs: 12000,
      concurrency: 16,
      queueCapacity: 100,
      connectionLimit: 8,
      autoscaleMaxConcurrency: 0,
      autoscaleQueueThreshold: 0,
      autoscaleDelayUs: 0,
      autoscaleIdleUs: 0,
      rateLimitRps: 0,
      hitRate: 0,
      hourlyCost: 0.4,
    },
    type: "default",
  },
];
const initialEdges: Edge[] = [
  { id: "e1", source: "client", target: "gateway" },
  { id: "e2", source: "gateway", target: "service" },
  { id: "e3", source: "service", target: "database" },
];
function templateNode(
  id: string,
  kind: string,
  x: number,
  y: number,
  overrides: Partial<Config> = {},
): SimNode {
  return {
    id,
    position: { x, y },
    type: "default",
    data: {
      kind,
      serviceTimeUs: 10_000,
      concurrency: 4,
      queueCapacity: 50,
      connectionLimit: 0,
      autoscaleMaxConcurrency: 0,
      autoscaleQueueThreshold: 0,
      autoscaleDelayUs: 0,
      autoscaleIdleUs: 0,
      rateLimitRps: 0,
      hitRate: 0,
      hourlyCost: 0.1,
      ...overrides,
    },
  };
}
const templates = [
  {
    name: "API baseline",
    description: "Gateway, service and database",
    nodes: initialNodes,
    edges: initialEdges,
    source: "client",
    rate: 100,
  },
  {
    name: "Read-through cache",
    description: "Compare cache hits with origin load",
    nodes: [
      templateNode("client", "client", 70, 210, {
        serviceTimeUs: 0,
        concurrency: 100,
        queueCapacity: 0,
        hourlyCost: 0,
      }),
      templateNode("cache", "cache", 350, 210, {
        hitRate: 0.8,
        concurrency: 20,
      }),
      templateNode("database", "database", 650, 210, {
        connectionLimit: 4,
        serviceTimeUs: 30_000,
        hourlyCost: 0.4,
      }),
    ],
    edges: [
      { id: "client-cache", source: "client", target: "cache" },
      { id: "cache-origin", source: "cache", target: "database" },
    ],
    source: "client",
    rate: 100,
  },
  {
    name: "Async workers",
    description: "Compare caller success with worker outcomes",
    nodes: [
      templateNode("client", "client", 70, 210, {
        serviceTimeUs: 0,
        concurrency: 100,
        queueCapacity: 0,
        hourlyCost: 0,
      }),
      templateNode("queue", "queue", 330, 210, {
        serviceTimeUs: 1000,
        concurrency: 8,
      }),
      templateNode("worker", "worker", 590, 210, {
        serviceTimeUs: 80_000,
        concurrency: 2,
        queueCapacity: 10,
      }),
      templateNode("database", "database", 850, 210, {
        connectionLimit: 2,
        serviceTimeUs: 20_000,
        hourlyCost: 0.4,
      }),
    ],
    edges: [
      { id: "client-queue", source: "client", target: "queue" },
      { id: "queue-worker", source: "queue", target: "worker" },
      { id: "worker-db", source: "worker", target: "database" },
    ],
    source: "client",
    rate: 20,
  },
];
const components = [
  { kind: "gateway", icon: Workflow },
  { kind: "load balancer", icon: Gauge },
  { kind: "service", icon: Server },
  { kind: "database", icon: Database },
  { kind: "cache", icon: Box },
  { kind: "queue", icon: Activity },
  { kind: "worker", icon: Server },
];

export default function Home() {
  const [nodes, setNodes] = useState<SimNode[]>(initialNodes);
  const [edges, setEdges] = useState<Edge[]>(initialEdges);
  const [selected, setSelected] = useState("service");
  const [selectedEdge, setSelectedEdge] = useState("");
  const [rate, setRate] = useState(100);
  const [duration, setDuration] = useState(10);
  const [seed, setSeed] = useState(42);
  const [maxApplicationRetries, setMaxApplicationRetries] = useState(0);
  const [applicationRetryBackoffUs, setApplicationRetryBackoffUs] = useState(0);
  const [source, setSource] = useState("client");
  const [result, setResult] = useState<Result | null>(null);
  const [error, setError] = useState("");
  const [validationError, setValidationError] = useState("");
  const [running, setRunning] = useState(false);
  const [saving, setSaving] = useState(false);
  const [projectId, setProjectId] = useState("");
  const [projectName, setProjectName] = useState("Untitled architecture");
  const [learningMode, setLearningMode] = useState(false);
  const importInputRef = useRef<HTMLInputElement | null>(null);
  const [projects, setProjects] = useState<ProjectSummary[]>([]);
  const [persistedRuns, setPersistedRuns] = useState(0);
  const [runs, setRuns] = useState<SavedRun[]>([]);
  const [selectedTrace, setSelectedTrace] = useState("");
  const traceLoadId = useRef(0);
  const [failures, setFailures] = useState<Failure[]>([]);
  const [repetitions, setRepetitions] = useState(10);
  const [sloP95Ms, setSloP95Ms] = useState(100);
  const [experiment, setExperiment] = useState<ExperimentResult | null>(null);
  const [experimentRunning, setExperimentRunning] = useState(false);
  const activeJobRef = useRef<ActiveJob | null>(null);
  const pollGeneration = useRef(0);
  const [jobProgress, setJobProgress] = useState<JobStatus | null>(null);
  const [baseline, setBaseline] = useState<Project["architecture"] | null>(
    null,
  );
  const [comparisonObjective, setComparisonObjective] = useState<
    "throughput" | "p95_latency" | "error_rate"
  >("throughput");
  const [comparison, setComparison] = useState<ComparisonResult | null>(null);
  const [comparisonRunning, setComparisonRunning] = useState(false);
  const [measuredThroughput, setMeasuredThroughput] = useState(50);
  const [measuredErrorRate, setMeasuredErrorRate] = useState(0);
  const [measuredP95Ms, setMeasuredP95Ms] = useState(100);
  const [calibration, setCalibration] = useState<CalibrationResult | null>(null);
  const [calibrationRunning, setCalibrationRunning] = useState(false);
  const history = useRef({
    past: [] as string[],
    current: "",
    future: [] as string[],
    timer: null as ReturnType<typeof setTimeout> | null,
    reset: false,
  });
  const [historyCounts, setHistoryCounts] = useState({ undo: 0, redo: 0 });
  const snapshot = JSON.stringify({
    nodes,
    edges,
    failures,
    source,
    rate,
    duration,
    seed,
    maxApplicationRetries,
    applicationRetryBackoffUs,
  });
  useEffect(() => {
    const h = history.current;
    if (h.reset || !h.current) {
      if (h.timer) clearTimeout(h.timer);
      h.past = [];
      h.current = snapshot;
      h.future = [];
      h.reset = false;
      setHistoryCounts({ undo: 0, redo: 0 });
      return;
    }
    if (snapshot === h.current) return;
    if (h.timer) clearTimeout(h.timer);
    h.timer = setTimeout(() => {
      h.past.push(h.current);
      if (h.past.length > 50) h.past.shift();
      h.current = snapshot;
      h.future = [];
      h.timer = null;
      setHistoryCounts({ undo: h.past.length, redo: 0 });
    }, 350);
    return () => {
      if (h.timer) clearTimeout(h.timer);
    };
  }, [snapshot]);
  const resetHistory = () => {
    history.current.reset = true;
  };
  const restoreSnapshot = (value: string) => {
    const state = JSON.parse(value) as {
      nodes: SimNode[];
      edges: Edge[];
      failures: Failure[];
      source: string;
      rate: number;
      duration: number;
      seed: number;
      maxApplicationRetries: number;
      applicationRetryBackoffUs: number;
    };
    setNodes(state.nodes);
    setEdges(state.edges);
    setFailures(state.failures);
    setSource(state.source);
    setRate(state.rate);
    setDuration(state.duration);
    setSeed(state.seed);
    setMaxApplicationRetries(state.maxApplicationRetries);
    setApplicationRetryBackoffUs(state.applicationRetryBackoffUs);
    setResult(null);
    setExperiment(null);
    setComparison(null);
  };
  const moveHistory = (direction: "undo" | "redo") => {
    const h = history.current;
    if (h.timer) {
      clearTimeout(h.timer);
      h.timer = null;
      if (snapshot !== h.current) {
        h.past.push(h.current);
        h.current = snapshot;
        h.future = [];
      }
    }
    if (direction === "undo" && h.past.length) {
      h.future.push(h.current);
      h.current = h.past.pop()!;
    } else if (direction === "redo" && h.future.length) {
      h.past.push(h.current);
      h.current = h.future.pop()!;
    } else return;
    restoreSnapshot(h.current);
    setHistoryCounts({ undo: h.past.length, redo: h.future.length });
  };
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target?.closest("input, textarea, select, [contenteditable=true]"))
        return;
      if (!(event.ctrlKey || event.metaKey) || event.altKey) return;
      const key = event.key.toLowerCase();
      if (key !== "z" && key !== "y") return;
      event.preventDefault();
      moveHistory(key === "y" || event.shiftKey ? "redo" : "undo");
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  });
  const chosen = nodes.find((n) => n.id === selected);
  const chosenEdge = edges.find((e) => e.id === selectedEdge);
  const resultTraces = result?.traces ?? [];
  const activeTrace =
    resultTraces.find((t) => t.id === selectedTrace) ?? resultTraces[0];
  const onNodesChange = useCallback(
    (changes: NodeChange<SimNode>[]) =>
      setNodes((ns) => applyNodeChanges(changes, ns)),
    [],
  );
  const onEdgesChange = useCallback(
    (changes: EdgeChange<Edge>[]) =>
      setEdges((es) => applyEdgeChanges(changes, es)),
    [],
  );
  const onConnect = useCallback(
    (connection: Connection) =>
      setEdges((es) =>
        addEdge(
          {
            ...connection,
            id: crypto.randomUUID(),
            data: {
              latencyUs: 1000,
              lossProbability: 0,
              maxRetries: 0,
              retryBackoffUs: 0,
              circuitFailureThreshold: 0,
              circuitOpenUs: 0,
              costPerMillionAttempts: 0,
            },
          },
          es,
        ),
      ),
    [],
  );
  const addNode = (kind: string) => {
    const id = `${kind.replaceAll(" ", "-")}-${nodes.length + 1}`;
    setNodes((ns) => [
      ...ns,
      {
        id,
        position: {
          x: 250 + (ns.length % 3) * 180,
          y: 100 + (ns.length % 4) * 90,
        },
        data: {
          kind,
          serviceTimeUs: 10000,
          concurrency: 4,
          queueCapacity: 50,
          connectionLimit: kind === "database" ? 4 : 0,
          transactionLockUs: 0,
          brokerCapacity: 0,
          maxDeliveries: 0,
          redeliveryDelayUs: 0,
          autoscaleMaxConcurrency: 0,
          autoscaleQueueThreshold: 0,
          autoscaleDelayUs: 0,
          autoscaleIdleUs: 0,
          rateLimitRps: 0,
          hitRate: kind === "cache" ? 0.8 : 0,
          hourlyCost:
            kind === "database"
              ? 0.4
              : kind === "service"
                ? 0.25
                : kind === "client"
                  ? 0
                  : 0.1,
        },
      },
    ]);
    setSelected(id);
  };
  const update = (key: keyof Config, value: number) =>
    setNodes((ns) =>
      ns.map((n) =>
        n.id === selected
          ? {
              ...n,
              data: {
                ...n.data,
                [key]: value,
                ...(key === "autoscaleMaxConcurrency"
                  ? value > 0
                    ? {
                        autoscaleQueueThreshold:
                          n.data.autoscaleQueueThreshold || 1,
                      }
                    : {
                        autoscaleQueueThreshold: 0,
                        autoscaleDelayUs: 0,
                        autoscaleIdleUs: 0,
                      }
                  : {}),
              },
            }
          : n,
      ),
    );
  const updateEdge = (
    key:
      | "latencyUs"
      | "lossProbability"
      | "maxRetries"
      | "retryBackoffUs"
      | "circuitFailureThreshold"
      | "circuitOpenUs"
      | "costPerMillionAttempts",
    value: number,
  ) =>
    setEdges((es) =>
      es.map((e) =>
        e.id === selectedEdge
          ? {
              ...e,
              data: {
                ...e.data,
                [key]: value,
                ...(key === "maxRetries" && value > 0 && !e.data?.retryBackoffUs
                  ? { retryBackoffUs: 1000 }
                  : {}),
                ...(key === "circuitFailureThreshold"
                  ? {
                      circuitOpenUs:
                        value > 0
                          ? Number(e.data?.circuitOpenUs || 500_000)
                          : 0,
                    }
                  : {}),
              },
            }
          : e,
      ),
    );
  const addFailure = (kind: Failure["kind"]) => {
    const targetId =
      kind === "node_outage" || kind === "node_degradation"
        ? selected || nodes[0]?.id
        : selectedEdge || edges[0]?.id;
    if (!targetId) return;
    setFailures((items) => [
      ...items,
      {
        id: crypto.randomUUID(),
        kind,
        targetId,
        startUs: Math.floor(duration / 2) * 1_000_000,
        durationUs: 1_000_000,
        serviceMultiplier: kind === "node_degradation" ? 2 : 0,
        lossProbability: kind === "edge_loss_spike" ? 0.5 : 0,
      },
    ]);
  };
  const updateFailure = (id: string, patch: Partial<Failure>) =>
    setFailures((items) =>
      items.map((f) => (f.id === id ? { ...f, ...patch } : f)),
    );
  const decorated = useMemo(
    () =>
      nodes.map((n) => ({
        ...n,
        data: {
          ...n.data,
          label: (
            <span className="node-label">
              <span className="node-kind">{n.data.kind}</span>
              <strong>{n.id}</strong>
              {result && (
                <small>
                  {Math.round((result.nodes[n.id]?.utilization ?? 0) * 100)}%
                  modeled utilization
                </small>
              )}
            </span>
          ),
        },
        className:
          result && (result.nodes[n.id]?.utilization ?? 0) > 0.85
            ? "hot-node"
            : "",
      })),
    [nodes, result],
  );
  const architecture = () => ({
    nodes: nodes.map((n) => ({
      id: n.id,
      ...n.data,
      positionX: n.position.x,
      positionY: n.position.y,
    })),
    edges: edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      latencyUs: Number(e.data?.latencyUs ?? 1000),
      lossProbability: Number(e.data?.lossProbability ?? 0),
      maxRetries: Number(e.data?.maxRetries ?? 0),
      retryBackoffUs: Number(e.data?.retryBackoffUs ?? 0),
      circuitFailureThreshold: Number(e.data?.circuitFailureThreshold ?? 0),
      circuitOpenUs: Number(e.data?.circuitOpenUs ?? 0),
      costPerMillionAttempts: Number(e.data?.costPerMillionAttempts ?? 0),
    })),
    workload: {
      source,
      requestsPerSecond: rate,
      durationSeconds: duration,
      seed,
      maxApplicationRetries,
      applicationRetryBackoffUs,
    },
    failures,
  });
  useEffect(() => {
    const controller = new AbortController();
    const timer = setTimeout(async () => {
      const input = architecture();
      try {
        const response = await fetch("/api/validate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            nodes: input.nodes.map(
              ({ positionX: _x, positionY: _y, ...node }) => node,
            ),
            edges: input.edges,
            workload: input.workload,
            failures: input.failures,
          }),
          signal: controller.signal,
        });
        const data = (await response.json()) as { error?: string };
        if (!controller.signal.aborted)
          setValidationError(
            response.ok ? "" : (data.error ?? "Model is invalid"),
          );
      } catch {
        if (!controller.signal.aborted)
          setValidationError("Validation unavailable");
      }
    }, 350);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [snapshot]);
  const refreshProjects = async () => {
    const response = await fetch("/api/projects");
    if (!response.ok) return;
    const data = (await response.json()) as ProjectSummary[];
    setProjects(data);
  };
  const openProject = async (id: string) => {
    traceLoadId.current++;
    setExperiment(null);
    const response = await fetch(`/api/projects/${id}`);
    const data = (await response.json()) as Project & { error?: string };
    if (!response.ok) throw new Error(data.error ?? "Could not open project");
    resetHistory();
    setNodes(
      data.architecture.nodes.map((n) => ({
        id: n.id,
        position: { x: n.positionX, y: n.positionY },
        data: {
          kind: n.kind,
          serviceTimeUs: n.serviceTimeUs,
          concurrency: n.concurrency,
          queueCapacity: n.queueCapacity,
          connectionLimit: n.connectionLimit ?? 0,
          transactionLockUs: n.transactionLockUs ?? 0,
          brokerCapacity: n.brokerCapacity ?? 0,
          maxDeliveries: n.maxDeliveries ?? 0,
          redeliveryDelayUs: n.redeliveryDelayUs ?? 0,
          autoscaleMaxConcurrency: n.autoscaleMaxConcurrency ?? 0,
          autoscaleQueueThreshold: n.autoscaleQueueThreshold ?? 0,
          autoscaleDelayUs: n.autoscaleDelayUs ?? 0,
          autoscaleIdleUs: n.autoscaleIdleUs ?? 0,
          rateLimitRps: n.rateLimitRps ?? 0,
          hitRate: n.hitRate ?? 0,
          hourlyCost: n.hourlyCost ?? 0,
        },
      })),
    );
    setEdges(
      data.architecture.edges.map((e) => ({
        id: e.id,
        source: e.source,
        target: e.target,
        data: {
          latencyUs: e.latencyUs,
          lossProbability: e.lossProbability,
          maxRetries: e.maxRetries ?? 0,
          retryBackoffUs: e.retryBackoffUs ?? 0,
          circuitFailureThreshold: e.circuitFailureThreshold ?? 0,
          circuitOpenUs: e.circuitOpenUs ?? 0,
          costPerMillionAttempts: e.costPerMillionAttempts ?? 0,
        },
      })),
    );
    setSource(data.architecture.workload.source);
    setRate(data.architecture.workload.requestsPerSecond);
    setDuration(data.architecture.workload.durationSeconds);
    setSeed(data.architecture.workload.seed);
    setMaxApplicationRetries(
      data.architecture.workload.maxApplicationRetries ?? 0,
    );
    setApplicationRetryBackoffUs(
      data.architecture.workload.applicationRetryBackoffUs ?? 0,
    );
    setFailures(data.architecture.failures ?? []);
    setProjectId(data.id);
    setProjectName(data.name);
    setResult(null);
    setSelectedTrace("");
    localStorage.setItem("networkforge-project-id", data.id);
    const history = await fetch(`/api/projects/${id}/runs`);
    if (history.ok) {
      const items = (await history.json()) as SavedRun[];
      setRuns(items);
      setPersistedRuns(items.length);
    }
  };
  useEffect(() => {
    const id = localStorage.getItem("networkforge-project-id");
    void refreshProjects();
    if (id)
      void openProject(id).catch(() =>
        localStorage.removeItem("networkforge-project-id"),
      );
  }, []);
  async function save() {
    setSaving(true);
    setError("");
    try {
      if (
        projectId &&
        projects.find((p) => p.id === projectId)?.name !== projectName
      ) {
        const rename = await fetch(`/api/projects/${projectId}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: projectName }),
        });
        if (!rename.ok)
          throw new Error((await rename.json()).error ?? "Rename failed");
      }
      const response = await fetch(
        projectId ? `/api/projects/${projectId}/versions` : "/api/projects",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(
            projectId
              ? architecture()
              : { name: projectName, architecture: architecture() },
          ),
        },
      );
      const data = await response.json();
      if (!response.ok) throw new Error(data.error ?? "Save failed");
      setProjectId(data.id);
      setProjectName(data.name);
      localStorage.setItem("networkforge-project-id", data.id);
      await refreshProjects();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Save failed");
    } finally {
      setSaving(false);
    }
  }
  async function watchJob(job: ActiveJob) {
    const generation = ++pollGeneration.current;
    activeJobRef.current = job;
    if (job.kind === "experiment") setExperimentRunning(true);
    else if (job.kind === "comparison") setComparisonRunning(true);
    else setRunning(true);
    let terminal = false;
    let failures = 0;
    try {
      while (generation === pollGeneration.current) {
        try {
          const response = await fetch(`/api/jobs/${job.id}`, {
            cache: "no-store",
          });
          const data = (await response.json()) as JobStatus;
          if (generation !== pollGeneration.current) return;
          if (!response.ok) {
            if (response.status === 404) {
              terminal = true;
              setError("Background job was lost after a server restart.");
              break;
            }
            throw new Error(data.error ?? "Could not load job progress");
          }
          failures = 0;
          setError("");
          setJobProgress(data);
          if (data.status === "succeeded") {
            terminal = true;
            if (job.kind === "experiment")
              setExperiment(data.result as ExperimentResult);
            else if (job.kind === "comparison")
              setComparison(data.result as ComparisonResult);
            else if (job.kind === "project_run") {
              const run = data.result as SavedRun;
              if (
                localStorage.getItem("networkforge-project-id") ===
                job.projectId
              ) {
                setResult(run.result);
                setSelectedTrace(run.result.traces?.[0]?.id ?? "");
                setRuns((items) => [
                  run,
                  ...items.filter((item) => item.id !== run.id),
                ]);
                setPersistedRuns((n) => n + 1);
              }
              void refreshProjects();
            } else {
              const result = data.result as Result;
              setResult(result);
              setSelectedTrace(result.traces?.[0]?.id ?? "");
            }
            break;
          }
          if (data.status === "failed" || data.status === "cancelled") {
            terminal = true;
            if (data.status === "failed")
              setError(data.error ?? "Background job failed");
            break;
          }
        } catch (e) {
          failures++;
          if (failures === 5)
            setError(
              e instanceof Error
                ? e.message
                : "Job progress unavailable; retrying.",
            );
        }
        await new Promise((resolve) =>
          setTimeout(resolve, failures ? 2000 : 300),
        );
      }
    } finally {
      if (generation === pollGeneration.current && terminal) {
        localStorage.removeItem("networkforge-active-job");
        activeJobRef.current = null;
        setJobProgress(null);
        setRunning(false);
        setExperimentRunning(false);
        setComparisonRunning(false);
      }
    }
  }
  async function submitJob(
    kind: ActiveJob["kind"],
    payload: object,
    id?: string,
  ) {
    if (activeJobRef.current)
      throw new Error("Finish or cancel the current background job first.");
    const response = await fetch("/api/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ kind, ...payload }),
    });
    const data = (await response.json()) as JobStatus & { error?: string };
    if (!response.ok)
      throw new Error(data.error ?? "Could not start background job");
    const job: ActiveJob = { id: data.id, kind, projectId: id };
    activeJobRef.current = job;
    localStorage.setItem("networkforge-active-job", JSON.stringify(job));
    void watchJob(job);
  }
  async function cancelCurrentJob() {
    const job = activeJobRef.current;
    if (!job) return;
    try {
      const response = await fetch(`/api/jobs/${job.id}`, { method: "DELETE" });
      if (!response.ok) throw new Error("Could not cancel job");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not cancel job");
    }
  }
  useEffect(() => {
    const saved = localStorage.getItem("networkforge-active-job");
    if (saved) {
      try {
        void watchJob(JSON.parse(saved) as ActiveJob);
      } catch {
        localStorage.removeItem("networkforge-active-job");
      }
    }
    return () => {
      pollGeneration.current++;
    };
  }, []);
  async function run() {
    traceLoadId.current++;
    setRunning(true);
    setError("");
    setResult(null);
    try {
      const input = architecture();
      if (projectId) {
        const saved = await fetch(`/api/projects/${projectId}/versions`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(input),
        });
        if (!saved.ok)
          throw new Error(
            (await saved.json()).error ?? "Saving the architecture failed",
          );
        await submitJob("project_run", { projectId }, projectId);
      } else {
        await submitJob("simulation", {
          simulation: {
            nodes: input.nodes.map(
              ({ positionX: _x, positionY: _y, ...n }) => n,
            ),
            edges: input.edges,
            workload: input.workload,
            failures: input.failures,
          },
        });
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Simulation failed");
      setRunning(false);
    }
  }
  async function runExperiment() {
    setExperimentRunning(true);
    setExperiment(null);
    setError("");
    try {
      const input = architecture();
      const simulation = {
        nodes: input.nodes.map(
          ({ positionX: _x, positionY: _y, ...node }) => node,
        ),
        edges: input.edges,
        workload: input.workload,
        failures: input.failures,
      };
      await submitJob("experiment", {
        experiment: {
          simulation,
          repetitions,
          sloP95Us: Math.round(sloP95Ms * 1000),
        },
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Experiment failed");
      setExperimentRunning(false);
    }
  }
  async function runComparison() {
    if (!baseline) return;
    setComparisonRunning(true);
    setComparison(null);
    setError("");
    try {
      const toSimulation = (snapshot: Project["architecture"]) => ({
        nodes: snapshot.nodes.map(
          ({ positionX: _x, positionY: _y, ...node }) => node,
        ),
        edges: snapshot.edges,
        workload: snapshot.workload,
        failures: snapshot.failures,
      });
      await submitJob("comparison", {
        comparison: {
          baseline: toSimulation(baseline),
          candidate: toSimulation(architecture()),
          repetitions,
          sloP95Us: Math.round(sloP95Ms * 1000),
          objective: comparisonObjective,
        },
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Comparison failed");
      setComparisonRunning(false);
    }
  }
  async function runCalibration() {
    setCalibrationRunning(true);
    setCalibration(null);
    setError("");
    try {
      const input = architecture();
      const response = await fetch("/api/calibrations", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          simulation: {
            nodes: input.nodes.map(({ positionX: _x, positionY: _y, ...node }) => node),
            edges: input.edges,
            workload: input.workload,
            failures: input.failures,
          },
          observed: {
            throughputRps: measuredThroughput,
            errorRate: measuredErrorRate / 100,
            p95Us: Math.round(measuredP95Ms * 1000),
          },
        }),
      });
      const data = (await response.json()) as CalibrationResult & { error?: string };
      if (!response.ok) throw new Error(data.error ?? "Calibration failed");
      setCalibration(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Calibration failed");
    } finally {
      setCalibrationRunning(false);
    }
  }
  async function viewRun(run: SavedRun) {
    const loadId = ++traceLoadId.current;
    setResult(run.result);
    setSelectedTrace("");
    const response = await fetch(
      `/api/projects/${projectId}/runs/${run.id}/traces`,
    );
    if (!response.ok && traceLoadId.current === loadId) {
      setError("Could not load sampled traces");
      return;
    }
    if (!response.ok) return;
    const traces = (await response.json()) as Trace[];
    if (traceLoadId.current !== loadId) return;
    setResult({ ...run.result, traces });
    setSelectedTrace(traces[0]?.id ?? "");
  }
  async function deleteProject() {
    traceLoadId.current++;
    if (
      !projectId ||
      !window.confirm(`Delete ${projectName} and its run history?`)
    )
      return;
    const response = await fetch(`/api/projects/${projectId}`, {
      method: "DELETE",
    });
    if (!response.ok) {
      setError((await response.json()).error ?? "Delete failed");
      return;
    }
    resetHistory();
    setProjectId("");
    setProjectName("Untitled architecture");
    setNodes(initialNodes);
    setEdges(initialEdges);
    setFailures([]);
    setSource("client");
    setRate(100);
    setDuration(10);
    setSeed(42);
    setMaxApplicationRetries(0);
    setApplicationRetryBackoffUs(0);
    setResult(null);
    setRuns([]);
    setPersistedRuns(0);
    localStorage.removeItem("networkforge-project-id");
    void refreshProjects();
  }
  function loadTemplate(template: (typeof templates)[number]) {
    if (activeJobRef.current) {
      setError("Finish or cancel the active job before changing templates.");
      return;
    }
    resetHistory();
    traceLoadId.current++;
    setProjectId("");
    localStorage.removeItem("networkforge-project-id");
    setProjectName(template.name);
    setSelected(template.nodes[0]?.id ?? "");
    setSelectedEdge("");
    setNodes(
      template.nodes.map((node) => ({
        ...node,
        position: { ...node.position },
        data: { ...node.data },
      })),
    );
    setEdges(
      template.edges.map((edge) => ({
        ...edge,
        data: edge.data ? { ...edge.data } : undefined,
      })),
    );
    setFailures([]);
    setSource(template.source);
    setRate(template.rate);
    setDuration(10);
    setSeed(42);
    setMaxApplicationRetries(0);
    setApplicationRetryBackoffUs(0);
    setResult(null);
    setExperiment(null);
    setComparison(null);
    setRuns([]);
    setPersistedRuns(0);
    setError("");
  }
  function exportArchitecture() {
    const payload = JSON.stringify(
      {
        format: "networkforge.architecture.v1",
        name: projectName,
        architecture: architecture(),
      },
      null,
      2,
    );
    const url = URL.createObjectURL(
      new Blob([payload], { type: "application/json" }),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = `${
      projectName
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, "") || "architecture"
    }.json`;
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  async function copyArchitecture() {
    try {
      await navigator.clipboard.writeText(
        JSON.stringify(
          {
            format: "networkforge.architecture.v1",
            name: projectName,
            architecture: architecture(),
          },
          null,
          2,
        ),
      );
      setError("");
    } catch {
      setError("Clipboard write failed. Use Export JSON instead.");
    }
  }
  async function importArchitecture(text: string) {
    if (activeJobRef.current)
      throw new Error("Finish or cancel the active job before importing.");
    const parsed = JSON.parse(text) as {
      format?: string;
      name?: string;
      architecture?: Project["architecture"];
    };
    if (
      parsed.format !== "networkforge.architecture.v1" ||
      !parsed.architecture ||
      !Array.isArray(parsed.architecture.nodes) ||
      !Array.isArray(parsed.architecture.edges)
    )
      throw new Error("This is not a NetworkForge architecture export.");
    const input = parsed.architecture;
    const response = await fetch("/api/validate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        nodes: input.nodes.map(
          ({ positionX: _x, positionY: _y, ...node }) => node,
        ),
        edges: input.edges,
        workload: input.workload,
        failures: input.failures ?? [],
      }),
    });
    const result = (await response.json()) as { error?: string };
    if (!response.ok)
      throw new Error(result.error ?? "Imported architecture is invalid");
    resetHistory();
    traceLoadId.current++;
    setProjectId("");
    localStorage.removeItem("networkforge-project-id");
    setProjectName((parsed.name ?? "Imported architecture").slice(0, 120));
    setSelected(input.nodes[0]?.id ?? "");
    setSelectedEdge("");
    setNodes(
      input.nodes.map((n) => ({
        id: n.id,
        position: { x: n.positionX ?? 0, y: n.positionY ?? 0 },
        data: {
          kind: n.kind,
          serviceTimeUs: n.serviceTimeUs ?? 0,
          concurrency: n.concurrency,
          queueCapacity: n.queueCapacity ?? 0,
          connectionLimit: n.connectionLimit ?? 0,
          transactionLockUs: n.transactionLockUs ?? 0,
          brokerCapacity: n.brokerCapacity ?? 0,
          maxDeliveries: n.maxDeliveries ?? 0,
          redeliveryDelayUs: n.redeliveryDelayUs ?? 0,
          autoscaleMaxConcurrency: n.autoscaleMaxConcurrency ?? 0,
          autoscaleQueueThreshold: n.autoscaleQueueThreshold ?? 0,
          autoscaleDelayUs: n.autoscaleDelayUs ?? 0,
          autoscaleIdleUs: n.autoscaleIdleUs ?? 0,
          rateLimitRps: n.rateLimitRps ?? 0,
          hitRate: n.hitRate ?? 0,
          hourlyCost: n.hourlyCost ?? 0,
        },
      })),
    );
    setEdges(
      input.edges.map((e) => ({
        id: e.id,
        source: e.source,
        target: e.target,
        data: {
          latencyUs: e.latencyUs,
          lossProbability: e.lossProbability,
          maxRetries: e.maxRetries,
          retryBackoffUs: e.retryBackoffUs,
          circuitFailureThreshold: e.circuitFailureThreshold,
          circuitOpenUs: e.circuitOpenUs,
          costPerMillionAttempts: e.costPerMillionAttempts,
        },
      })),
    );
    setFailures(input.failures ?? []);
    setSource(input.workload.source);
    setRate(input.workload.requestsPerSecond);
    setDuration(input.workload.durationSeconds);
    setSeed(input.workload.seed);
    setMaxApplicationRetries(input.workload.maxApplicationRetries ?? 0);
    setApplicationRetryBackoffUs(input.workload.applicationRetryBackoffUs ?? 0);
    setResult(null);
    setExperiment(null);
    setComparison(null);
    setRuns([]);
    setPersistedRuns(0);
    setError("");
  }
  async function pasteArchitecture() {
    try {
      await importArchitecture(await navigator.clipboard.readText());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Clipboard import failed");
    }
  }
  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark">
            <Workflow size={18} />
          </span>
          <strong>NetworkForge</strong>
          <span className="tag">LAB</span>
        </div>
        <span className="crumb">
          Workspace <ArrowRight size={13} /> {projectName}
        </span>
        <div className="top-actions">
          <span className="saved">
            {projectId
              ? `Saved project · ${persistedRuns} runs`
              : "Unsaved draft"}
          </span>
          <button className="button" onClick={save} disabled={saving}>
            <Save size={15} />
            {saving ? "Saving…" : "Save"}
          </button>
          <button
            className="button button-primary"
            onClick={run}
            disabled={running || experimentRunning || comparisonRunning}
          >
            <Play size={15} fill="currentColor" />
            {running ? "Running…" : "Run simulation"}
          </button>
          {running && (
            <button className="button" onClick={() => void cancelCurrentJob()}>
              Cancel run
            </button>
          )}
        </div>
      </header>
      <div className="workspace">
        <aside className="left-panel">
          <div className="panel-heading">
            <span>PROJECT</span>
          </div>
          <input
            className="project-name"
            aria-label="Project name"
            value={projectName}
            onChange={(e) => setProjectName(e.target.value)}
          />
          <select
            className="project-select"
            aria-label="Open project"
            value={projectId}
            onChange={(e) => {
              if (e.target.value)
                void openProject(e.target.value).catch((err) =>
                  setError(String(err)),
                );
              else {
                resetHistory();
                traceLoadId.current++;
                setProjectId("");
                setProjectName("Untitled architecture");
                setNodes(initialNodes);
                setEdges(initialEdges);
                setFailures([]);
                setSource("client");
                setRate(100);
                setDuration(10);
                setSeed(42);
                setMaxApplicationRetries(0);
                setApplicationRetryBackoffUs(0);
                setResult(null);
                setRuns([]);
                localStorage.removeItem("networkforge-project-id");
                setPersistedRuns(0);
              }
            }}
          >
            <option value="">New draft</option>
            {projects.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name} · v{p.version}
              </option>
            ))}
          </select>
          {projectId && (
            <button className="delete-project" onClick={deleteProject}>
              Delete project
            </button>
          )}
          <div className="panel-heading">
            <span>TEMPLATES</span>
          </div>
          <div className="template-list">
            {templates.map((template) => (
              <button
                key={template.name}
                onClick={() => loadTemplate(template)}
              >
                <strong>{template.name}</strong>
                <small>{template.description}</small>
              </button>
            ))}
          </div>
          <div className="file-actions">
            <button onClick={exportArchitecture}>Export JSON</button>
            <button onClick={() => importInputRef.current?.click()}>
              Import JSON
            </button>
            <button onClick={() => void copyArchitecture()}>Copy</button>
            <button onClick={() => void pasteArchitecture()}>Paste</button>
          </div>
          <input
            ref={importInputRef}
            type="file"
            accept="application/json,.json"
            className="hidden-import"
            aria-label="Import architecture JSON"
            onChange={(event) => {
              const file = event.target.files?.[0];
              event.target.value = "";
              if (!file) return;
              if (file.size > 1_000_000) {
                setError("Architecture file is too large.");
                return;
              }
              void file
                .text()
                .then(importArchitecture)
                .catch((e) =>
                  setError(e instanceof Error ? e.message : "Import failed"),
                );
            }}
          />
          <button
            className="learning-toggle"
            onClick={() => setLearningMode((value) => !value)}
            aria-pressed={learningMode}
          >
            {learningMode ? "Hide learning guide" : "Show learning guide"}
          </button>
          {learningMode && (
            <div className="sidebar-note">
              <strong>Learning guide</strong>
              <p>
                Start with a template, run it, then change one setting and
                compare against a captured baseline. Queue wait shows where work
                stalls. Caller success at an async queue does not mean its
                worker completed. Cost is based on your configured rates.
              </p>
            </div>
          )}
          <div className="panel-heading">
            <span>COMPONENT LIBRARY</span>
            <CircleHelp size={15} />
          </div>
          <p className="panel-hint">
            Add a component, then connect its handles on the canvas.
          </p>
          <div className="component-list">
            {components.map(({ kind, icon: Icon }) => (
              <button key={kind} onClick={() => addNode(kind)}>
                <span className="component-icon">
                  <Icon size={17} />
                </span>
                <span>{kind}</span>
                <Plus size={14} />
              </button>
            ))}
          </div>
          <div className="sidebar-note">
            <strong>Model scope</strong>
            <p>
              Gateways enforce rate limits, load balancers route round robin,
              caches bypass their origin on seeded hits, and databases cap
              active connections. A queue acknowledges after enqueue processing
              and sends a background job to its connected worker. Other nodes
              use finite service capacity and FIFO queues.
            </p>
          </div>
        </aside>
        <section className="center">
          <div className="canvas-toolbar">
            <span className="eyebrow">ARCHITECTURE CANVAS</span>
            <span className="canvas-count">
              {nodes.length} components · {edges.length} connections
            </span>
            <button
              aria-label="Undo architecture edit"
              title="Undo (Ctrl+Z)"
              disabled={historyCounts.undo === 0 && !history.current.timer}
              onClick={() => moveHistory("undo")}
            >
              <CornerUpLeft size={15} />
            </button>
            <button
              aria-label="Redo architecture edit"
              title="Redo (Ctrl+Y)"
              disabled={historyCounts.redo === 0}
              onClick={() => moveHistory("redo")}
            >
              <CornerUpRight size={15} />
            </button>
            <button
              onClick={() => {
                setNodes(initialNodes);
                setEdges(initialEdges);
                setFailures([]);
                setResult(null);
              }}
              title="Reset example"
            >
              <RotateCcw size={15} />
            </button>
          </div>
          <div className="canvas">
            <ReactFlow
              nodes={decorated}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              onNodeClick={(_, node) => {
                setSelected(node.id);
                setSelectedEdge("");
              }}
              onEdgeClick={(_, edge) => {
                setSelectedEdge(edge.id);
                setSelected("");
              }}
              fitView
              fitViewOptions={{ padding: 0.2 }}
            >
              <Background color="#293342" gap={20} size={1} />
              <Controls />
              <MiniMap pannable zoomable />
            </ReactFlow>
          </div>
          <div className="console">
            <div className="console-title">
              <span className="status-dot" /> SIMULATION CONSOLE{" "}
              <span className="console-mode">DETERMINISTIC · SEED {seed}</span>
            </div>
            {error && <p className="error">{error}</p>}
            {validationError && (
              <p className="error" role="status">
                Model check: {validationError}
              </p>
            )}
            {!validationError && !error && (
              <p className="panel-hint">Model ready for simulation.</p>
            )}
            {result ? (
              <div className="console-content">
                <span>
                  Processed {result.eventsProcessed.toLocaleString()} events
                </span>
                <span>{result.completed.toLocaleString()} completed</span>
                <span>{result.failed.toLocaleString()} failed</span>
                <span>{result.timedOut.toLocaleString()} timed out</span>
              </div>
            ) : (
              <p className="console-empty">
                Configure the workload and run the architecture to inspect
                measured results.
              </p>
            )}
            {jobProgress && (
              <p className="panel-hint" role="status">
                {jobProgress.kind ?? activeJobRef.current?.kind} job{" "}
                {jobProgress.status}: {jobProgress.progressDone}/
                {jobProgress.progressTotal} steps
              </p>
            )}
          </div>
        </section>
        <aside className="right-panel">
          <div className="panel-heading">SIMULATION SETUP</div>
          <label>
            Source component
            <select value={source} onChange={(e) => setSource(e.target.value)}>
              {nodes.map((n) => (
                <option key={n.id} value={n.id}>
                  {n.id}
                </option>
              ))}
            </select>
          </label>
          <label>
            Requests / second
            <input
              type="number"
              min="1"
              max="10000"
              value={rate}
              onChange={(e) => setRate(Number(e.target.value))}
            />
          </label>
          <label>
            Duration / seconds
            <input
              type="number"
              min="1"
              max="60"
              value={duration}
              onChange={(e) => setDuration(Number(e.target.value))}
            />
          </label>
          <label>
            Random seed
            <input
              type="number"
              value={seed}
              onChange={(e) => setSeed(Number(e.target.value))}
            />
          </label>
          <label>
            Application retries (0 to 3)
            <input
              type="number"
              min="0"
              max="3"
              value={maxApplicationRetries}
              onChange={(e) => {
                const retries = Number(e.target.value);
                setMaxApplicationRetries(retries);
                if (retries > 0 && applicationRetryBackoffUs === 0)
                  setApplicationRetryBackoffUs(100_000);
                if (retries === 0) setApplicationRetryBackoffUs(0);
              }}
            />
          </label>
          {maxApplicationRetries > 0 && (
            <label>
              Application retry backoff (microseconds)
              <input
                type="number"
                min="1"
                value={applicationRetryBackoffUs}
                onChange={(e) =>
                  setApplicationRetryBackoffUs(Number(e.target.value))
                }
              />
            </label>
          )}
          <div className="panel-divider" />
          <div className="panel-heading">FAILURE SCHEDULE</div>
          <div className="failure-actions">
            <button onClick={() => addFailure("node_outage")}>
              + Node outage
            </button>
            <button onClick={() => addFailure("edge_partition")}>
              + Edge partition
            </button>
            <button onClick={() => addFailure("node_degradation")}>
              + Slow node
            </button>
            <button onClick={() => addFailure("edge_loss_spike")}>
              + Link loss
            </button>
          </div>
          {failures.length === 0 && (
            <p className="panel-hint">No failures scheduled.</p>
          )}
          {failures.map((f) => (
            <div className="failure-item" key={f.id}>
              <div className="failure-title">
                <strong>
                  {
                    {
                      node_outage: "Node outage",
                      edge_partition: "Edge partition",
                      node_degradation: "Slow node",
                      edge_loss_spike: "Link loss",
                    }[f.kind]
                  }
                </strong>
                <button
                  aria-label="Remove failure"
                  onClick={() =>
                    setFailures((items) =>
                      items.filter((item) => item.id !== f.id),
                    )
                  }
                >
                  Remove
                </button>
              </div>
              <label>
                Target
                <select
                  value={f.targetId}
                  onChange={(e) =>
                    updateFailure(f.id, { targetId: e.target.value })
                  }
                >
                  {(f.kind === "node_outage" || f.kind === "node_degradation"
                    ? nodes
                    : edges
                  ).map((item) => (
                    <option key={item.id} value={item.id}>
                      {item.id}
                    </option>
                  ))}
                </select>
              </label>
              {f.kind === "node_degradation" && (
                <label>
                  Service time multiplier
                  <input
                    type="number"
                    min="1.01"
                    max="10"
                    step="0.1"
                    value={f.serviceMultiplier ?? 2}
                    onChange={(e) =>
                      updateFailure(f.id, {
                        serviceMultiplier: Number(e.target.value),
                      })
                    }
                  />
                </label>
              )}
              {f.kind === "edge_loss_spike" && (
                <label>
                  Additional loss probability
                  <input
                    type="number"
                    min="0.01"
                    max="1"
                    step="0.05"
                    value={f.lossProbability ?? 0.5}
                    onChange={(e) =>
                      updateFailure(f.id, {
                        lossProbability: Number(e.target.value),
                      })
                    }
                  />
                </label>
              )}
              <div className="failure-times">
                <label>
                  Start (s)
                  <input
                    type="number"
                    min="0"
                    max={duration}
                    step="0.1"
                    value={f.startUs / 1_000_000}
                    onChange={(e) =>
                      updateFailure(f.id, {
                        startUs: Math.round(Number(e.target.value) * 1_000_000),
                      })
                    }
                  />
                </label>
                <label>
                  Length (s)
                  <input
                    type="number"
                    min="0.1"
                    max="60"
                    step="0.1"
                    value={f.durationUs / 1_000_000}
                    onChange={(e) =>
                      updateFailure(f.id, {
                        durationUs: Math.round(
                          Number(e.target.value) * 1_000_000,
                        ),
                      })
                    }
                  />
                </label>
              </div>
            </div>
          ))}
          <div className="panel-divider" />
          <div className="panel-heading">REPEATED-SEED EXPERIMENT</div>
          <p className="panel-hint">
            Run the same model with consecutive seeds to measure run-to-run
            variation.
          </p>
          <label>
            Runs
            <select
              value={repetitions}
              onChange={(e) => setRepetitions(Number(e.target.value))}
            >
              <option value={10}>10 runs</option>
              <option value={50}>50 runs</option>
              <option value={100}>100 runs</option>
            </select>
          </label>
          <label>
            P95 latency target (ms)
            <input
              type="number"
              min="0.001"
              max="60000"
              step="1"
              value={sloP95Ms}
              onChange={(e) => setSloP95Ms(Number(e.target.value))}
            />
          </label>
          <button
            className="experiment-button"
            disabled={(running || comparisonRunning) && !experimentRunning}
            onClick={
              experimentRunning
                ? () => void cancelCurrentJob()
                : () => void runExperiment()
            }
          >
            {experimentRunning ? "Cancel experiment" : "Run experiment"}
          </button>
          {experiment && (
            <div className="experiment-result">
              <div className="experiment-numbers">
                <div>
                  <small>MEAN THROUGHPUT</small>
                  <strong>
                    {experiment.summary.meanThroughputRps.toFixed(1)} req/s
                  </strong>
                </div>
                <div>
                  <small>MEAN ERROR RATE</small>
                  <strong>
                    {(experiment.summary.meanErrorRate * 100).toFixed(1)}%
                  </strong>
                </div>
                <div>
                  <small>SLO VIOLATION</small>
                  <strong>
                    {(experiment.summary.sloViolationProbability * 100).toFixed(
                      1,
                    )}
                    %
                  </strong>
                </div>
                <div>
                  <small>MEAN RUN P95</small>
                  <strong>
                    {experiment.summary.completedLatencyRuns
                      ? (experiment.summary.meanP95Us / 1000).toFixed(1) + " ms"
                      : "No completions"}
                  </strong>
                </div>
              </div>
              <p className="panel-hint">
                Mean modeled cost: $
                {experiment.summary.meanEstimatedScenarioCost.toFixed(4)} per
                scenario, $
                {experiment.summary.meanEstimatedHourlyCost.toFixed(2)} per hour
                at this load. Cost per success:{" "}
                {experiment.summary.costPerSuccessfulRequest == null
                  ? "n/a"
                  : `$${experiment.summary.costPerSuccessfulRequest.toFixed(5)}`}
                .
              </p>
              {experiment.summary.completedLatencyRuns > 0 && (
                <p className="panel-hint">
                  Observed run P95 range (5th–95th percentile):{" "}
                  {(experiment.summary.outcomeP05P95Us / 1000).toFixed(1)}–
                  {(experiment.summary.outcomeP95P95Us / 1000).toFixed(1)} ms.
                  Bootstrap 95% interval for mean run P95:{" "}
                  {(experiment.summary.meanP95ConfidenceLowUs / 1000).toFixed(
                    1,
                  )}
                  –
                  {(experiment.summary.meanP95ConfidenceHighUs / 1000).toFixed(
                    1,
                  )}{" "}
                  ms, from {experiment.summary.completedLatencyRuns} runs with
                  completions.
                </p>
              )}
              <div className="chart-title">Error rate by seed</div>
              <div className="experiment-chart">
                <ResponsiveContainer width="100%" height={170}>
                  <BarChart
                    data={experiment.runs.map((run, index) => ({
                      trial: index + 1,
                      errorPercent: Number((run.errorRate * 100).toFixed(2)),
                    }))}
                    margin={{ top: 8, right: 4, bottom: 4, left: -20 }}
                    accessibilityLayer
                  >
                    <CartesianGrid stroke="#344153" vertical={false} />
                    <XAxis dataKey="trial" hide />
                    <YAxis tick={{ fontSize: 10, fill: "#93a4b7" }} />
                    <Tooltip
                      contentStyle={{
                        background: "#1b2531",
                        border: "1px solid #45546a",
                        fontSize: 11,
                      }}
                    />
                    <Bar
                      dataKey="errorPercent"
                      name="Error rate (%)"
                      fill="#8292f0"
                      maxBarSize={16}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}
          <div className="panel-divider" />
          <div className="panel-heading">ARCHITECTURE COMPARISON</div>
          <p className="panel-hint">
            Capture a baseline, edit or open another architecture, then compare
            with the same workload and seed sequence.
          </p>
          <button
            className="experiment-button"
            onClick={() => {
              setBaseline(architecture());
              setComparison(null);
            }}
          >
            {baseline ? "Replace baseline" : "Capture baseline"}
          </button>
          {baseline && (
            <>
              <p className="panel-hint">
                Baseline captured: {baseline.nodes.length} nodes,{" "}
                {baseline.edges.length} edges.
              </p>
              <label>
                Objective
                <select
                  value={comparisonObjective}
                  onChange={(e) =>
                    setComparisonObjective(
                      e.target.value as typeof comparisonObjective,
                    )
                  }
                >
                  <option value="throughput">Maximize throughput</option>
                  <option value="p95_latency">Minimize P95 latency</option>
                  <option value="error_rate">Minimize error rate</option>
                </select>
              </label>
              <button
                className="experiment-button"
                disabled={(running || experimentRunning) && !comparisonRunning}
                onClick={
                  comparisonRunning
                    ? () => void cancelCurrentJob()
                    : () => void runComparison()
                }
              >
                {comparisonRunning ? "Cancel comparison" : "Compare variants"}
              </button>
            </>
          )}
          {comparison && (
            <div className="comparison-result">
              <strong>
                Winner for {comparison.objective.replaceAll("_", " ")}:{" "}
                {comparison.winner}
              </strong>
              <table>
                <thead>
                  <tr>
                    <th>Metric</th>
                    <th>Baseline</th>
                    <th>Candidate</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>Throughput</td>
                    <td>
                      {comparison.baseline.summary.meanThroughputRps.toFixed(1)}
                    </td>
                    <td>
                      {comparison.candidate.summary.meanThroughputRps.toFixed(
                        1,
                      )}
                    </td>
                  </tr>
                  <tr>
                    <td>Error rate</td>
                    <td>
                      {(
                        comparison.baseline.summary.meanErrorRate * 100
                      ).toFixed(1)}
                      %
                    </td>
                    <td>
                      {(
                        comparison.candidate.summary.meanErrorRate * 100
                      ).toFixed(1)}
                      %
                    </td>
                  </tr>
                  <tr>
                    <td>Mean run P95</td>
                    <td>
                      {comparison.baseline.summary.completedLatencyRuns
                        ? (
                            comparison.baseline.summary.meanP95Us / 1000
                          ).toFixed(1) + " ms"
                        : "n/a"}
                    </td>
                    <td>
                      {comparison.candidate.summary.completedLatencyRuns
                        ? (
                            comparison.candidate.summary.meanP95Us / 1000
                          ).toFixed(1) + " ms"
                        : "n/a"}
                    </td>
                  </tr>
                  <tr>
                    <td>SLO miss</td>
                    <td>
                      {(
                        comparison.baseline.summary.sloViolationProbability *
                        100
                      ).toFixed(1)}
                      %
                    </td>
                    <td>
                      {(
                        comparison.candidate.summary.sloViolationProbability *
                        100
                      ).toFixed(1)}
                      %
                    </td>
                  </tr>
                  <tr>
                    <td>Modeled $/h</td>
                    <td>
                      {comparison.baseline.summary.meanEstimatedHourlyCost.toFixed(
                        2,
                      )}
                    </td>
                    <td>
                      {comparison.candidate.summary.meanEstimatedHourlyCost.toFixed(
                        2,
                      )}
                    </td>
                  </tr>
                </tbody>
              </table>
              <p className="panel-hint">
                Compared {comparison.baseline.summary.repetitions} runs per
                variant using identical base traffic settings.{" "}
                {comparison.comparedPairs > 0 && (
                  <>
                    Paired candidate-minus-baseline difference:{" "}
                    {comparison.objective === "p95_latency"
                      ? `${(comparison.objectiveDelta / 1000).toFixed(2)} ms`
                      : comparison.objective === "error_rate"
                        ? `${(comparison.objectiveDelta * 100).toFixed(2)} percentage points`
                        : `${comparison.objectiveDelta.toFixed(2)} req/s`}
                    . Bootstrap 95% interval:{" "}
                    {comparison.objective === "p95_latency"
                      ? `${(comparison.objectiveDeltaConfidenceLow / 1000).toFixed(2)} to ${(comparison.objectiveDeltaConfidenceHigh / 1000).toFixed(2)} ms`
                      : comparison.objective === "error_rate"
                        ? `${(comparison.objectiveDeltaConfidenceLow * 100).toFixed(2)} to ${(comparison.objectiveDeltaConfidenceHigh * 100).toFixed(2)} percentage points`
                        : `${comparison.objectiveDeltaConfidenceLow.toFixed(2)} to ${comparison.objectiveDeltaConfidenceHigh.toFixed(2)} req/s`}
                    .
                  </>
                )}
              </p>
            </div>
          )}
          <div className="panel-divider" />
          <div className="panel-heading">MEASURED DATA CALIBRATION</div>
          <p className="panel-hint">Enter measurements from traffic with the same rate and duration as this workload. The fit scales all non-client service times together.</p>
          <label>Measured throughput (requests / second)
            <input type="number" min="0" step="0.1" value={measuredThroughput} onChange={(e) => setMeasuredThroughput(Number(e.target.value))} />
          </label>
          <label>Measured error rate (%)
            <input type="number" min="0" max="100" step="0.1" value={measuredErrorRate} onChange={(e) => setMeasuredErrorRate(Number(e.target.value))} />
          </label>
          <label>Measured P95 latency (ms)
            <input type="number" min="0" step="0.1" value={measuredP95Ms} onChange={(e) => setMeasuredP95Ms(Number(e.target.value))} />
          </label>
          <button className="experiment-button" disabled={calibrationRunning} onClick={() => void runCalibration()}>{calibrationRunning ? "Calibrating…" : "Compare with measurements"}</button>
          {calibration && <div className="cost-summary" aria-label="Calibration report">
            <strong>Best service-time scale: {calibration.serviceTimeMultiplier}×</strong>
            <span>Baseline score: {calibration.baseline.score.toFixed(3)} · fitted score: {calibration.calibrated.score.toFixed(3)}</span>
            <span>Fitted throughput: {calibration.calibrated.throughputRps.toFixed(1)} req/s · error: {(calibration.calibrated.errorRate * 100).toFixed(1)}% · P95: {(calibration.calibrated.p95Us / 1000).toFixed(1)} ms</span>
            <small>Fit across {calibration.candidatesEvaluated} candidate scales. Check against separate measurements before using it for planning.</small>
          </div>}
          <div className="panel-divider" />
          <div className="panel-heading">PROPERTIES</div>
          {chosenEdge ? (
            <>
              <div className="selected-name">
                <span className="component-icon">
                  <Workflow size={18} />
                </span>
                <div>
                  <strong>
                    {chosenEdge.source} to {chosenEdge.target}
                  </strong>
                  <small>Network connection</small>
                </div>
              </div>
              <label>
                Latency (microseconds)
                <input
                  type="number"
                  min="0"
                  value={Number(chosenEdge.data?.latencyUs ?? 1000)}
                  onChange={(e) =>
                    updateEdge("latencyUs", Number(e.target.value))
                  }
                />
              </label>
              <label>
                Packet loss probability (0 to 1)
                <input
                  type="number"
                  min="0"
                  max="1"
                  step="0.01"
                  value={Number(chosenEdge.data?.lossProbability ?? 0)}
                  onChange={(e) =>
                    updateEdge("lossProbability", Number(e.target.value))
                  }
                />
              </label>
              <label>
                Maximum retries (0 to 5)
                <input
                  type="number"
                  min="0"
                  max="5"
                  value={Number(chosenEdge.data?.maxRetries ?? 0)}
                  onChange={(e) =>
                    updateEdge("maxRetries", Number(e.target.value))
                  }
                />
              </label>
              {Number(chosenEdge.data?.maxRetries ?? 0) > 0 && (
                <label>
                  Initial retry backoff (microseconds)
                  <input
                    type="number"
                    min="1"
                    value={Number(chosenEdge.data?.retryBackoffUs ?? 1000)}
                    onChange={(e) =>
                      updateEdge("retryBackoffUs", Number(e.target.value))
                    }
                  />
                </label>
              )}
              <label>
                Circuit failure threshold (0 disables)
                <input
                  type="number"
                  min="0"
                  value={Number(chosenEdge.data?.circuitFailureThreshold ?? 0)}
                  onChange={(e) =>
                    updateEdge(
                      "circuitFailureThreshold",
                      Number(e.target.value),
                    )
                  }
                />
              </label>
              {Number(chosenEdge.data?.circuitFailureThreshold ?? 0) > 0 && (
                <label>
                  Circuit open duration (microseconds)
                  <input
                    type="number"
                    min="1"
                    value={Number(chosenEdge.data?.circuitOpenUs ?? 500_000)}
                    onChange={(e) =>
                      updateEdge("circuitOpenUs", Number(e.target.value))
                    }
                  />
                </label>
              )}
              <label>
                Cost per million send attempts (synthetic $)
                <input
                  type="number"
                  min="0"
                  step="0.01"
                  value={Number(chosenEdge.data?.costPerMillionAttempts ?? 0)}
                  onChange={(e) =>
                    updateEdge("costPerMillionAttempts", Number(e.target.value))
                  }
                />
              </label>
            </>
          ) : chosen ? (
            <>
              <div className="selected-name">
                <span className="component-icon">
                  <Server size={18} />
                </span>
                <div>
                  <strong>{chosen.id}</strong>
                  <small>{chosen.data.kind}</small>
                </div>
              </div>
              <label>
                Service time (µs)
                <input
                  type="number"
                  min="0"
                  value={chosen.data.serviceTimeUs}
                  onChange={(e) =>
                    update("serviceTimeUs", Number(e.target.value))
                  }
                />
              </label>
              <label>
                Concurrent slots
                <input
                  type="number"
                  min="1"
                  value={chosen.data.concurrency}
                  onChange={(e) =>
                    update("concurrency", Number(e.target.value))
                  }
                />
              </label>
              <label>
                Queue capacity
                <input
                  type="number"
                  min="0"
                  value={chosen.data.queueCapacity}
                  onChange={(e) =>
                    update("queueCapacity", Number(e.target.value))
                  }
                />
              </label>
              <label>
                Scale-out maximum slots (0 disables)
                <input
                  type="number"
                  min="0"
                  value={chosen.data.autoscaleMaxConcurrency}
                  onChange={(e) =>
                    update("autoscaleMaxConcurrency", Number(e.target.value))
                  }
                />
              </label>
              {chosen.data.autoscaleMaxConcurrency > 0 && (
                <>
                  <label>
                    Scale out at queued requests
                    <input
                      type="number"
                      min="1"
                      value={chosen.data.autoscaleQueueThreshold}
                      onChange={(e) =>
                        update(
                          "autoscaleQueueThreshold",
                          Number(e.target.value),
                        )
                      }
                    />
                  </label>
                  <label>
                    Scale-out delay (microseconds)
                    <input
                      type="number"
                      min="0"
                      value={chosen.data.autoscaleDelayUs}
                      onChange={(e) =>
                        update("autoscaleDelayUs", Number(e.target.value))
                      }
                    />
                  </label>
                  <label>
                    Scale-in idle delay (microseconds; 0 disables)
                    <input
                      type="number"
                      min="0"
                      value={chosen.data.autoscaleIdleUs}
                      onChange={(e) =>
                        update("autoscaleIdleUs", Number(e.target.value))
                      }
                    />
                  </label>
                </>
              )}
              {chosen.data.kind === "database" && (<>
                <label>
                  Connection limit (0 uses concurrent slots)
                  <input
                    type="number"
                    min="0"
                    value={chosen.data.connectionLimit}
                    onChange={(e) =>
                      update("connectionLimit", Number(e.target.value))
                    }
                  />
                </label>
                <label>Exclusive transaction lock hold (microseconds)
                  <input type="number" min="0" max={chosen.data.serviceTimeUs} value={chosen.data.transactionLockUs ?? 0} onChange={(e) => update("transactionLockUs", Number(e.target.value))} />
                </label>
              </>)}
              {chosen.data.kind === "queue" && (<>
                <label>Broker message capacity (0 unlimited)
                  <input type="number" min="0" value={chosen.data.brokerCapacity ?? 0} onChange={(e) => update("brokerCapacity", Number(e.target.value))} />
                </label>
                <label>Maximum delivery attempts (0 uses 1)
                  <input type="number" min="0" max="5" value={chosen.data.maxDeliveries ?? 0} onChange={(e) => update("maxDeliveries", Number(e.target.value))} />
                </label>
                {(chosen.data.maxDeliveries ?? 0) > 1 && <label>Redelivery delay (microseconds)
                  <input type="number" min="1" value={chosen.data.redeliveryDelayUs ?? 0} onChange={(e) => update("redeliveryDelayUs", Number(e.target.value))} />
                </label>}
              </>)}
              <label>
                Configured node cost per hour (synthetic $)
                <input
                  type="number"
                  min="0"
                  step="0.01"
                  value={chosen.data.hourlyCost}
                  onChange={(e) => update("hourlyCost", Number(e.target.value))}
                />
              </label>
              {chosen.data.kind === "gateway" && (
                <label>
                  Rate limit (requests / second; 0 disables)
                  <input
                    type="number"
                    min="0"
                    value={chosen.data.rateLimitRps}
                    onChange={(e) =>
                      update("rateLimitRps", Number(e.target.value))
                    }
                  />
                </label>
              )}
              {chosen.data.kind === "cache" && (
                <label>
                  Cache hit probability (0 to 1)
                  <input
                    type="number"
                    min="0"
                    max="1"
                    step="0.05"
                    value={chosen.data.hitRate}
                    onChange={(e) => update("hitRate", Number(e.target.value))}
                  />
                </label>
              )}
              {result?.nodes[chosen.id] && (
                <p className="panel-hint">
                  {chosen.data.kind === "database"
                    ? `${Math.round(result.nodes[chosen.id].connectionUtilization * 100)}% connection utilization · ${result.nodes[chosen.id].connectionWaits} waits · ${result.nodes[chosen.id].connectionRejected} rejected`
                    : chosen.data.kind === "gateway"
                      ? `${result.nodes[chosen.id].rateLimited} requests rate limited`
                      : chosen.data.kind === "cache"
                        ? `${result.nodes[chosen.id].cacheHits} hits · ${result.nodes[chosen.id].cacheMisses} misses`
                        : `${result.nodes[chosen.id].completed} operations completed`}
                </p>
              )}
              {result?.nodes[chosen.id] &&
                chosen.data.autoscaleMaxConcurrency > 0 && (
                  <p className="panel-hint">
                    Scaling: {result.nodes[chosen.id].scaleUps} out ·{" "}
                    {result.nodes[chosen.id].scaleDowns} in · peak{" "}
                    {result.nodes[chosen.id].peakConcurrency} slots.
                  </p>
                )}
            </>
          ) : (
            <p className="panel-hint">
              Select a component to edit its capacity.
            </p>
          )}
          {result && (
            <>
              <div className="panel-divider" />
              <div className="panel-heading">RUN RESULTS</div>
              {(result.failuresTriggered > 0 ||
                result.retries > 0 ||
                result.applicationRetries > 0 ||
                result.circuitOpens > 0) && (
                <p className="panel-hint">
                  {result.failuresTriggered} failures triggered;{" "}
                  {Object.values(result.edgeDrops ?? {}).reduce(
                    (a, b) => a + b,
                    0,
                  )}{" "}
                  failed transmissions; {result.retries} transport retries;{" "}
                  {result.applicationRetries} application retries (
                  {result.applicationRetryExhausted} exhausted);{" "}
                  {result.circuitOpens} circuit openings;{" "}
                  {result.circuitRejected} circuit rejections.
                </p>
              )}
              <div className="metric-grid">
                <div>
                  <small>THROUGHPUT</small>
                  <strong>
                    {(result.completed / duration).toFixed(1)} <em>req/s</em>
                  </strong>
                </div>
                <div>
                  <small>ERROR RATE</small>
                  <strong>
                    {(
                      (100 * (result.failed + result.timedOut)) /
                      result.generated
                    ).toFixed(1)}
                    <em>%</em>
                  </strong>
                </div>
                <div>
                  <small>P95 LATENCY</small>
                  <strong>
                    {(result.p95Us / 1000).toFixed(1)} <em>ms</em>
                  </strong>
                </div>
                <div>
                  <small>P99 LATENCY</small>
                  <strong>
                    {(result.p99Us / 1000).toFixed(1)} <em>ms</em>
                  </strong>
                </div>
              </div>
              {result.asyncEnqueued > 0 && (
                <p className="panel-hint">
                  Background jobs: {result.asyncEnqueued} enqueued ·{" "}
                  {result.backgroundCompleted} completed ·{" "}
                  {result.backgroundFailed} failed · {result.backgroundTimedOut}{" "}
                  timed out · {result.backgroundRedeliveries ?? 0} redeliveries · {result.backgroundDeadLetters ?? 0} dead letters. Caller results above end at enqueue.
                </p>
              )}
              <div className="cost-summary">
                <strong>Modeled cost</strong>
                <span>
                  Scenario: ${result.estimatedScenarioCost.toFixed(4)}
                </span>
                <span>
                  Hourly at this load: ${result.estimatedHourlyCost.toFixed(2)}
                </span>
                <span>
                  Per successful request:{" "}
                  {result.costPerSuccessfulRequest == null
                    ? "n/a"
                    : `$${result.costPerSuccessfulRequest.toFixed(5)}`}
                </span>
                <small>
                  Configured base rates scaled by active slots; not a cloud
                  bill.
                </small>
              </div>
              <div className="panel-divider" />
              <div className="panel-heading">ARCHITECTURE REVIEW</div>
              {result.findings?.length ? (
                <ol className="finding-list">
                  {result.findings.map((finding) => (
                    <li key={finding.id} className="finding-card">
                      <div className="finding-heading">
                        <strong>{finding.title}</strong>
                        <span>{finding.severity}</span>
                      </div>
                      {finding.componentId && (
                        <small>Component: {finding.componentId}</small>
                      )}
                      <div className="finding-evidence">
                        {finding.evidence.map((item) => (
                          <span key={item.metric}>
                            {item.metric.replaceAll("_", " ")}:{" "}
                            {Number(item.value.toFixed(2))} {item.unit}
                          </span>
                        ))}
                      </div>
                      <p>{finding.recommendation}</p>
                      <small>{finding.expectedImpact}</small>
                      <small>Tradeoff: {finding.tradeoffs}</small>
                    </li>
                  ))}
                </ol>
              ) : (
                <p className="panel-hint">
                  No review rule triggered for this run.
                </p>
              )}
            </>
          )}
          {resultTraces.length > 0 && activeTrace && (
            <>
              <div className="panel-divider" />
              <div className="panel-heading">SAMPLED REQUEST TRACE</div>
              <label>
                Request
                <select
                  value={activeTrace.id}
                  onChange={(e) => setSelectedTrace(e.target.value)}
                >
                  {resultTraces.map((trace) => (
                    <option key={trace.id} value={trace.id}>
                      {trace.id} · {trace.status}
                    </option>
                  ))}
                </select>
              </label>
              <p className="trace-summary">
                {activeTrace.status} ·{" "}
                {(activeTrace.latencyUs / 1000).toFixed(2)} ms modeled latency
                {activeTrace.truncated ? " · event list truncated" : ""}
              </p>
              <ol className="trace-events">
                {activeTrace.events.map((event, index) => (
                  <li key={`${index}-${event.kind}`}>
                    <time>{(event.timeUs / 1000).toFixed(2)} ms</time>
                    <span>{event.kind.replaceAll("_", " ")}</span>
                    {event.componentId && <small>{event.componentId}</small>}
                  </li>
                ))}
              </ol>
            </>
          )}
          {runs.length > 0 && (
            <>
              <div className="panel-divider" />
              <div className="panel-heading">SAVED RUNS</div>
              <div className="run-list">
                {runs.map((run) => (
                  <button key={run.id} onClick={() => void viewRun(run)}>
                    <span>
                      Run #{run.id} · v{run.version}
                    </span>
                    <small>
                      P95 {(run.result.p95Us / 1000).toFixed(1)} ms ·{" "}
                      {run.result.completed} completed
                    </small>
                  </button>
                ))}
              </div>
            </>
          )}
        </aside>
      </div>
    </main>
  );
}
