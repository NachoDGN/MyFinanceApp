import assert from "node:assert/strict";
import test from "node:test";

import {
  getImportTemplateInferenceConfig,
  inferImportTemplateDraft,
} from "../packages/ingestion/src/import-template-inference.ts";

test("import template inference falls back to the configured rules model", () => {
  const previousImportModel = process.env.OPENAI_IMPORT_TEMPLATE_MODEL;
  const previousRulesModel = process.env.OPENAI_RULES_MODEL;

  delete process.env.OPENAI_IMPORT_TEMPLATE_MODEL;
  process.env.OPENAI_RULES_MODEL = "gpt-5.4";

  try {
    assert.equal(getImportTemplateInferenceConfig().model, "gpt-5.4");
  } finally {
    if (previousImportModel === undefined) {
      delete process.env.OPENAI_IMPORT_TEMPLATE_MODEL;
    } else {
      process.env.OPENAI_IMPORT_TEMPLATE_MODEL = previousImportModel;
    }

    if (previousRulesModel === undefined) {
      delete process.env.OPENAI_RULES_MODEL;
    } else {
      process.env.OPENAI_RULES_MODEL = previousRulesModel;
    }
  }
});

test("inferImportTemplateDraft builds an xlsx template from table-start and column inference", async () => {
  let previewTableInput: Record<string, unknown> | null = null;

  const template = await inferImportTemplateDraft(
    {
      userId: "user-1",
      account: {
        id: "account-1",
        institutionName: "Santander",
        accountType: "checking",
        defaultCurrency: "EUR",
      },
      filePath: "/tmp/santander-apr.xlsx",
      originalFilename: "santander-apr.xlsx",
    },
    {
      llmClient: {
        async generateText() {
          throw new Error("Not used in this test.");
        },
        async generateJson({ schemaName }) {
          if (schemaName === "spreadsheet_table_start") {
            return {
              sheet_name: "Movements",
              header_row_index: 4,
              rows_to_skip_before_header: 3,
              start_column_letter: "C",
            };
          }
          if (schemaName === "spreadsheet_layout") {
            return {
              column_map: {
                transaction_date: "Date",
                description_raw: "Description",
                amount_original_signed: "Amount",
                balance_original: "Balance",
              },
              sign_logic: {
                mode: "signed_amount",
                invert_sign: false,
              },
              date_day_first: true,
            };
          }
          throw new Error(`Unexpected schema ${schemaName}`);
        },
      },
      inspectWorkbook: async () => ({
        fileKind: "xlsx",
        delimiter: null,
        encoding: null,
        sheetPreviews: [
          {
            sheetName: "Summary",
            previewCsv: "row,A,B,C\n1,Statement summary,,",
          },
          {
            sheetName: "Movements",
            previewCsv: "row,A,B,C,D\n1,,,\n4,,,Date,Description",
          },
        ],
      }),
      previewTable: async (input) => {
        previewTableInput = input as unknown as Record<string, unknown>;
        return {
          sheetName: "Movements",
          previewCsv:
            "Date,Description,Amount,Balance\n01/04/2026,Coffee,-3.50,100.00",
          headers: ["Date", "Description", "Amount", "Balance"],
        };
      },
    },
  );

  assert.equal(previewTableInput?.headerRowIndex, 4);
  assert.equal(previewTableInput?.rowsToSkipBeforeHeader, 3);
  assert.equal(previewTableInput?.startColumnIndex, 2);
  assert.equal(previewTableInput?.sheetName, "Movements");

  assert.equal(template.name, "Santander checking santander-apr auto");
  assert.equal(template.fileKind, "xlsx");
  assert.equal(template.sheetName, "Movements");
  assert.equal(template.headerRowIndex, 4);
  assert.equal(template.rowsToSkipBeforeHeader, 3);
  assert.equal(template.delimiter, null);
  assert.deepEqual(template.columnMapJson, {
    transaction_date: "Date",
    description_raw: "Description",
    amount_original_signed: "Amount",
    balance_original: "Balance",
  });
  assert.deepEqual(template.signLogicJson, {
    mode: "signed_amount",
    invert_sign: false,
  });
  assert.deepEqual(template.normalizationRulesJson, {
    date_day_first: true,
    start_column_index: 2,
    start_column_letter: "C",
  });
});

test("inferImportTemplateDraft preserves csv dialect details and debit-credit sign logic", async () => {
  const template = await inferImportTemplateDraft(
    {
      userId: "user-1",
      account: {
        id: "account-1",
        institutionName: "BBVA",
        accountType: "credit_card",
        defaultCurrency: "EUR",
      },
      filePath: "/tmp/bbva.csv",
      originalFilename: "bbva.csv",
    },
    {
      llmClient: {
        async generateText() {
          throw new Error("Not used in this test.");
        },
        async generateJson({ schemaName }) {
          if (schemaName === "spreadsheet_table_start") {
            return {
              header_row_index: 6,
              rows_to_skip_before_header: 5,
              start_column_letter: "A",
            };
          }
          if (schemaName === "spreadsheet_layout") {
            return {
              column_map: {
                transaction_date: "Fecha",
                description_raw: "Concepto",
                posted_date: "Fecha valor",
              },
              sign_logic: {
                mode: "debit_credit_columns",
                debit_column: "Cargo",
                credit_column: "Abono",
              },
              date_day_first: true,
            };
          }
          throw new Error(`Unexpected schema ${schemaName}`);
        },
      },
      inspectWorkbook: async () => ({
        fileKind: "csv",
        delimiter: ";",
        encoding: "cp1252",
        sheetPreviews: [
          {
            sheetName: null,
            previewCsv: "row,A,B,C\n1,Cuenta Visa,,",
          },
        ],
      }),
      previewTable: async () => ({
        sheetName: null,
        previewCsv:
          "Fecha,Fecha valor,Concepto,Cargo,Abono\n03/04/2026,03/04/2026,Coffee,3,",
        headers: ["Fecha", "Fecha valor", "Concepto", "Cargo", "Abono"],
      }),
    },
  );

  assert.equal(template.fileKind, "csv");
  assert.equal(template.delimiter, ";");
  assert.equal(template.encoding, "cp1252");
  assert.deepEqual(template.columnMapJson, {
    transaction_date: "Fecha",
    description_raw: "Concepto",
    posted_date: "Fecha valor",
  });
  assert.deepEqual(template.signLogicJson, {
    mode: "debit_credit_columns",
    debit_column: "Cargo",
    credit_column: "Abono",
  });
  assert.deepEqual(template.normalizationRulesJson, {
    date_day_first: true,
    start_column_index: 0,
    start_column_letter: "A",
  });
});

test("inferImportTemplateDraft resolves inferred xlsx sheet names to an existing preview sheet", async () => {
  let previewTableInput: Record<string, unknown> | null = null;

  const template = await inferImportTemplateDraft(
    {
      userId: "user-1",
      account: {
        id: "account-1",
        institutionName: "MyInvestor",
        accountType: "brokerage_account",
        defaultCurrency: "EUR",
      },
      filePath: "/tmp/myinvestor.xlsx",
      originalFilename: "myinvestor.xlsx",
    },
    {
      llmClient: {
        async generateText() {
          throw new Error("Not used in this test.");
        },
        async generateJson({ schemaName }) {
          if (schemaName === "spreadsheet_table_start") {
            return {
              sheet_name: "Movimientos MyInvestor",
              header_row_index: 2,
              rows_to_skip_before_header: 1,
              start_column_letter: "A",
            };
          }
          if (schemaName === "spreadsheet_layout") {
            return {
              column_map: {
                transaction_date: "Fecha",
                description_raw: "Concepto",
                amount_original_signed: "Importe",
              },
              sign_logic: {
                mode: "signed_amount",
                invert_sign: false,
              },
              date_day_first: true,
            };
          }
          throw new Error(`Unexpected schema ${schemaName}`);
        },
      },
      inspectWorkbook: async () => ({
        fileKind: "xlsx",
        delimiter: null,
        encoding: null,
        sheetPreviews: [
          {
            sheetName: "Movimientos",
            previewCsv:
              "row,A,B,C\n1,MyInvestor movimientos,,\n2,Fecha,Concepto,Importe",
          },
        ],
      }),
      previewTable: async (input) => {
        previewTableInput = input as unknown as Record<string, unknown>;
        return {
          sheetName: "Movimientos",
          previewCsv: "Fecha,Concepto,Importe\n03/04/2026,Compra ETF,-100.00",
          headers: ["Fecha", "Concepto", "Importe"],
        };
      },
    },
  );

  assert.equal(previewTableInput?.sheetName, "Movimientos");
  assert.equal(template.sheetName, "Movimientos");
});

test("inferImportTemplateDraft explains when an xlsx has no worksheet previews", async () => {
  await assert.rejects(
    () =>
      inferImportTemplateDraft(
        {
          userId: "user-1",
          account: {
            id: "account-1",
            institutionName: "Broker",
            accountType: "brokerage_account",
            defaultCurrency: "EUR",
          },
          filePath: "/tmp/chart-only.xlsx",
          originalFilename: "chart-only.xlsx",
        },
        {
          llmClient: {
            async generateText() {
              throw new Error("Not used in this test.");
            },
            async generateJson() {
              throw new Error("Not used in this test.");
            },
          },
          inspectWorkbook: async () => ({
            fileKind: "xlsx",
            delimiter: null,
            encoding: null,
            sheetPreviews: [],
          }),
        },
      ),
    /does not contain any worksheet tabs with rows and columns to preview/i,
  );
});

test("inferImportTemplateDraft passes the reference date into layout inference", async () => {
  let layoutPrompt = "";

  await inferImportTemplateDraft(
    {
      userId: "user-1",
      account: {
        id: "account-1",
        institutionName: "MyInvestor",
        accountType: "brokerage_account",
        defaultCurrency: "EUR",
      },
      filePath: "/tmp/myinvestor.xlsx",
      originalFilename: "myinvestor.xlsx",
    },
    {
      referenceDate: "2026-04-04",
      llmClient: {
        async generateText() {
          throw new Error("Not used in this test.");
        },
        async generateJson({ schemaName, userPrompt }) {
          if (schemaName === "spreadsheet_table_start") {
            return {
              sheet_name: "Movimientos",
              header_row_index: 1,
              rows_to_skip_before_header: 0,
              start_column_letter: "A",
            };
          }
          if (schemaName === "spreadsheet_layout") {
            layoutPrompt = userPrompt;
            return {
              column_map: {
                transaction_date: "Fecha",
                description_raw: "Concepto",
                amount_original_signed: "Importe",
              },
              sign_logic: {
                mode: "signed_amount",
                invert_sign: false,
              },
              date_day_first: false,
            };
          }
          throw new Error(`Unexpected schema ${schemaName}`);
        },
      },
      inspectWorkbook: async () => ({
        fileKind: "xlsx",
        delimiter: null,
        encoding: null,
        sheetPreviews: [
          {
            sheetName: "Movimientos",
            previewCsv: "row,A,B,C\n1,Fecha,Concepto,Importe",
          },
        ],
      }),
      previewTable: async () => ({
        sheetName: "Movimientos",
        previewCsv: "Fecha,Concepto,Importe\n03/12/2026,Compra ETF,-100.00",
        headers: ["Fecha", "Concepto", "Importe"],
      }),
    },
  );

  assert.match(layoutPrompt, /Reference date: 2026-04-04/);
  assert.match(layoutPrompt, /03\/12\/2026/);
});
