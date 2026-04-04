import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";

const repository = createFinanceRepository();
const domain = new FinanceDomainService(repository);
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
      console.log(`[${timestamp}] worker idle`);
      return;
    }
    console.log(
      `[${timestamp}] worker processed ${processed} jobs: ${result.processedJobs
        .map((job) => `${job.jobType}:${job.status}`)
        .join(", ")}`,
    );
  } finally {
    isRunning = false;
  }
}

async function main() {
  console.log("[worker] started");
  await runWorkerCycle();
  setInterval(() => {
    void runWorkerCycle();
  }, 15000);
}

void main();
