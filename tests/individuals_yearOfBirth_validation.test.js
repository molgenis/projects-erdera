import { expect, test, describe } from "vitest";
import validateYearOfBirth from "../src/js/individuals_yearOfBirth_validation";

describe("year of birth", () => {
  const currentYear = new Date().getUTCFullYear();
  const nextYear = currentYear + 1;

  test("year cannot be later than the current year", () => {
    expect(validateYearOfBirth(nextYear)).toContain("Year of birth");
  });

  test("year cannot be earlier than 1900", () => {
    expect(validateYearOfBirth(1800)).toContain("Year of birth");
    expect(validateYearOfBirth(180)).toContain("Year of birth");
  });
  
  test("year is valid", () => {
    expect(validateYearOfBirth(currentYear - 1)).toBeTruthy();
  });
});
