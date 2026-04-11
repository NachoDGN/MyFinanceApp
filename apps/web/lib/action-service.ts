import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService } from "@myfinance/domain";

export const repository = createFinanceRepository();
export const domain = new FinanceDomainService(repository);
