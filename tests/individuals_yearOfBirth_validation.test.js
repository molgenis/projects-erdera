import { expect, test, describe } from "vitest";
import validateYearOfBirth from "../src/js/individuals_yearOfBirth_validation";

describe("year of birth", () => {
  const currentYear = new Date().getUTCFullYear();
  const nextYear = currentYear + 1;

  test("year must be less than the current year", () => {
    expect(validateYearOfBirth(nextYear)).toContain("Year of birth");
  });

  test("year must be more than 1900", () => {
    expect(validateYearOfBirth(1800)).toContain("Year of birth");
  });

  test("year of birth is valid", () => {
    expect(validateYearOfBirth(currentYear - 1)).toBeTruthy();
  });
});
