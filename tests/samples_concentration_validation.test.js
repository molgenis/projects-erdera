import { expect, test, describe } from "vitest";
import validateConcentration from "../src/js/samples_concentration_validation";

describe("concentration measurement", () => {
  test("value cannot be less than the minimum concentration (90)", () => {
    expect(validateConcentration(80)).toContain("Concentration cannot be less");
  });

  test("value cannot be less than maximum concentration (150)", () => {
    expect(validateConcentration(160)).toContain(
      "Concentration cannot be greater"
    );
  });

  test("concentration is valid", () => {
    expect(validateConcentration(100)).toBeTruthy();
  });
});
