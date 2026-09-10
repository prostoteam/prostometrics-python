// Generates protocol v5 golden fixtures using the official Node client's encoder,
// so the Python client can be checked against a real implementation.
//
// The encoder is not part of the Node package's published entry point, so the
// path to a built copy of it is given on the command line:
//
//   node tests/golden/generate.mjs <prostometrics-node>/dist/dictionary.js \
//     > tests/golden/protocol_v5.json
//
// Regenerate only when the wire format itself changes, and always from the Node
// client rather than from this one: the fixture exists to catch this client
// drifting away from the others, which it cannot do if it grades its own work.
import { pathToFileURL } from "node:url";

const encoderPath = process.argv[2];
if (!encoderPath) {
  console.error("usage: node generate.mjs <path to the Node client's dist/dictionary.js>");
  process.exit(2);
}
const { encodeLinePayloadV5, newDictionaryState } = await import(pathToFileURL(encoderPath).href);

const TS = 1700000000;

const cases = [
  {
    name: "single_counter_no_labels",
    batches: [{ counters: [{ metric: "requests", value: 1, labels: [], timestamp: TS }], values: [], uniques: [] }],
  },
  {
    name: "counter_with_sorted_labels",
    batches: [{ counters: [{ metric: "requests", value: 7, labels: ["method=GET", "status=200"], timestamp: TS }], values: [], uniques: [] }],
  },
  {
    name: "all_event_kinds",
    batches: [{
      counters: [{ metric: "c_metric", value: 3, labels: [], timestamp: TS }],
      values: [
        { metric: "v_metric", value: 1.5, sparse: false, labels: [], timestamp: TS + 1 },
        { metric: "s_metric", value: 2.5, sparse: true, labels: [], timestamp: TS + 2 },
        { metric: "o_metric", value: 100, sparse: false, success: true, labels: [], timestamp: TS + 4 },
        { metric: "o_metric", value: 0, sparse: false, success: true, labels: [], timestamp: TS + 5 },
      ],
      uniques: [{ metric: "u_metric", uniqueID: "18446744073709551615", labels: [], timestamp: TS + 3 }],
    }],
  },
  {
    name: "integral_and_fractional_samples",
    batches: [{
      counters: [],
      values: [
        { metric: "v", value: 2048, sparse: false, labels: [], timestamp: TS },
        { metric: "v", value: 0.125, sparse: false, labels: [], timestamp: TS + 1 },
        { metric: "v", value: 0, sparse: false, labels: [], timestamp: TS + 2 },
      ],
      uniques: [],
    }],
  },
  {
    name: "counter_rounding_half_away_from_zero",
    batches: [{
      counters: [
        { metric: "a", value: 0.5, labels: [], timestamp: TS },
        { metric: "b", value: 1.5, labels: [], timestamp: TS },
        { metric: "c", value: 2.5, labels: [], timestamp: TS },
      ],
      values: [], uniques: [],
    }],
  },
  {
    name: "second_batch_reuses_dictionary",
    batches: [
      { counters: [{ metric: "requests", value: 1, labels: ["m=GET"], timestamp: TS }], values: [], uniques: [] },
      { counters: [{ metric: "requests", value: 2, labels: ["m=GET"], timestamp: TS + 1 }], values: [], uniques: [] },
    ],
  },
  {
    name: "third_batch_adds_a_series_and_bumps_revision",
    batches: [
      { counters: [{ metric: "a", value: 1, labels: [], timestamp: TS }], values: [], uniques: [] },
      { counters: [{ metric: "a", value: 1, labels: [], timestamp: TS + 1 }], values: [], uniques: [] },
      { counters: [{ metric: "b", value: 1, labels: [], timestamp: TS + 2 }], values: [], uniques: [] },
    ],
  },
  {
    name: "unicode_metric_and_label_values",
    batches: [{ counters: [{ metric: "заказы", value: 1, labels: ["город=Москва"], timestamp: TS }], values: [], uniques: [] }],
  },
  {
    name: "eight_labels",
    batches: [{
      counters: [{ metric: "wide", value: 1, labels: Array.from({ length: 8 }, (_, i) => `l${i}=v${i}`), timestamp: TS }],
      values: [], uniques: [],
    }],
  },
];

const out = [];
for (const testCase of cases) {
  const state = newDictionaryState();
  const bodies = [];
  for (const batch of testCase.batches) {
    const payload = { batchID: "fixed-batch", counters: batch.counters, values: batch.values, uniques: batch.uniques };
    const body = encodeLinePayloadV5(payload, state, false).toString("utf8");
    bodies.push(body.split(state.sessionID).join("<SESSION>"));
  }
  out.push({ name: testCase.name, batches: testCase.batches, bodies });
}
console.log(JSON.stringify({ generator: "prostometrics-node dist/dictionary.js", cases: out }, null, 2));
