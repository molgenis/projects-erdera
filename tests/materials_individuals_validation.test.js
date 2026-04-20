import { expect, test, describe } from "vitest";
import validateIndividuals from "../src/js/material_individuals_validation";

const singleRecord = [{ id: "Test-1" }];
const multipleRecords = [{ id: "Test-1" }, { id: "Test-2" }];

describe("Individuals in materials", () => {
  test("Samples are not linked to more than one individual", () => {
    expect(validateIndividuals("RD3.Samples RNA", singleRecord)).toBeTruthy();
    expect(validateIndividuals("RD3.Samples RNA", multipleRecords)).toContain(
      "Samples can only"
    );

    expect(validateIndividuals("RD3.Samples OGM", singleRecord)).toBeTruthy();
    expect(validateIndividuals("RD3.Samples OGM", multipleRecords)).toContain(
      "Samples can only"
    );

    expect(validateIndividuals("RD3.Samples srDNA", singleRecord)).toBeTruthy();
    expect(validateIndividuals("RD3.Samples srDNA", multipleRecords)).toContain(
      "Samples can only"
    );

    expect(validateIndividuals("RD3.Samples lrGS", singleRecord)).toBeTruthy();
    expect(validateIndividuals("RD3.Samples lrGS", multipleRecords)).toContain(
      "Samples can only"
    );
  });

  test("Consent is not linked to more than one individual", () => {
    expect(
      validateIndividuals("RD3.Individual consent", multipleRecords)
    ).toEqual("Consent can only be linked to one individual");
    expect(
      validateIndividuals("RD3.Individual consent", singleRecord)
    ).toBeTruthy();
  });

  test("Other tables can accept multiple links", () => {
    expect(validateIndividuals("RD3.Files", singleRecord)).toBeTruthy();
    expect(validateIndividuals("RD3.Files", multipleRecords)).toBeTruthy();

    expect(validateIndividuals("RD3.Experiments", singleRecord)).toBeTruthy();
    expect(
      validateIndividuals("RD3.Experiments", multipleRecords)
    ).toBeTruthy();
  });
});
