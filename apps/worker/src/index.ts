import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";

const repository = createFinanceRepository();
const domain = new FinanceDomainService(repository);
const DEFAULT_WORKER_CONCURRENCY = 4;
const WORKER_POLL_INTERVAL_MS = 15_000;

function readWorkerConcurrency() {
  const parsed = Number.parseInt(process.env.WORKER_CONCURRENCY ?? "", 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return DEFAULT_WORKER_CONCURRENCY;
  }
  return Math.floor(parsed);
}

function createWorkerLane(laneNumber: number) {
  let isRunning = false;

  async function runWorkerCycle() {
    if (isRunning) {
      return;
    }

    isRunning = true;
    try {
      const result = await domain.runPendingJobs(true);
      const processed = result.processedJobs.length;
      const timestamp = new Date().toISOString();
      if (processed === 0) {
        console.log(`[${timestamp}] worker lane ${laneNumber} idle`);
        return;
      }
      console.log(
        `[${timestamp}] worker lane ${laneNumber} processed ${processed} jobs: ${result.processedJobs
          .map((job) => `${job.jobType}:${job.status}`)
          .join(", ")}`,
      );
    } finally {
      isRunning = false;
    }
  }

  return {
    runWorkerCycle,
  };
}

async function main() {
  const concurrency = readWorkerConcurrency();
  console.log(`[worker] started with ${concurrency} lanes`);

  const lanes = Array.from({ length: concurrency }, (_, index) =>
    createWorkerLane(index + 1),
  );

  await Promise.all(lanes.map((lane) => lane.runWorkerCycle()));
  for (const lane of lanes) {
    setInterval(() => {
      void lane.runWorkerCycle();
    }, WORKER_POLL_INTERVAL_MS);
  }
}

void main();
