/**
 * @name query_individuals
 * @description
 * Retrieve metadata from the graphql to ensure all metadata the individual to
 * files hierarchy still works. We want to test for the following conditions:
 *
 * 1. The number of individuals in the demo dataset has not changed and can be retrieved
 * 2. Samples, files, and consent can still be retrieved via materials
 * 3. Experiments can still be accessed through samples
 * 4. Files are accessible in materials and experiments (also through samples)
 * 
 * To run this test, follow these steps:
 * 
 * 1. Open the tests/onfig.ts file and set the host and schema. It is recommend
 *    to run these tests against the dev branch (i.e., localhost). Alternatively,
 *    you run the tests against a preview server; make sure the schema is public.
 * 2. Run the test: `yarn vitest tests/graphql/query_individuals.spec.ts
 * 3. If any of the tests fail, please resolve the issues.
 *      a. Check to see if your changes did not break the model hierarchy
 *      b. Check for changes in the model (i.e., column names, table names)
 *      c. If there were breaking changes, make sure these are done with
 *         good intent, and then update these tests.
 */

import { expect, test, describe } from "vitest";
import { graphqlApi } from "../config";

interface QueryResponse {
  data: {
    Individuals: Record<string, any>[];
  };
}

type Individual = Record<string, any>;

const query = `query {
    Individuals(filter:{includedInResources: { id: { equals: "EGAD00001008392" }}}) {
    id
    materials {
      id
      mg_tableclass

      usedInExperiments {
        id
        mg_tableclass
        outputFiles {
            id
        }
      }
    }
  }
}`;

let individualsData: Individual[];

describe("GraphQL: Individuals", () => {
  test.beforeAll(async () => {
    const response = await fetch(graphqlApi, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query: query }),
    });
    const data = (await response.json()) as QueryResponse;
    individualsData = data.data.Individuals;
  });

  test("Individuals: There are 18 individuals in the example dataset", () => {
    const pids = individualsData.map((row) => row.id);
    expect(pids.length).toEqual(18);
  });

  test("Materials: The first index has 4 samples, 25 files, and 1 consent", () => {
    const proband = individualsData.filter((row) => row.id === "Case1C");
    const materials = proband[0].materials;
    const samples = materials.filter((row: Record<string, any>) =>
      row.mg_tableclass.includes(".Samples")
    );
    const files = materials.filter((row: Record<string, any>) =>
      row.mg_tableclass.includes(".Files")
    );
    const consent = materials.filter((row: Record<string, any>) =>
      row.mg_tableclass.includes(".Individual consent")
    );

    expect(consent.length).toEqual(1);
    expect(samples.length).toEqual(4);
    expect(files.length).toEqual(25);
  });

  test("Experiments: records are accessible through samples", () => {
    const experiments = individualsData
      .filter((row) => row.id === "Case1C")[0]
      .materials.filter((row: Record<string, any>) =>
        row.mg_tableclass.includes(".Samples")
      )
      .map((row: Record<string, any>) => [
        ...row.usedInExperiments.map((row: Record<string, any>) => row.id),
      ]);
    expect(experiments.length).toEqual(4);
  });

  test("Files: data is accessible through samples and experiments", () => {
    const files = individualsData
      .filter((row) => row.id === "Case1C")[0]
      .materials.filter(
        (row: Record<string, any>) =>
          row.mg_tableclass.includes(".Samples") && row.usedInExperiments
      )
      .filter((row: Record<string, any>) => row.usedInExperiments)
      .reduce((acc: number, item: Record<string, any>) => {
        const hasOuputFiles = item.usedInExperiments.find(
          (row: Record<string, any>) => row.outputFiles
        );
        if (hasOuputFiles) {
          acc += 1;
        }
        return acc;
      }, 0);
    expect(files).toBeGreaterThan(0);
  });
});
