/**
 * @name Individuals Year of birth validation
 * @description
 * This validation expresses determines if the year&mdash;an integer&mdash;is
 * greater than the current year or earlier than fixed year (which is set to 1900).
 * This forces users to enter year as `YYYY`, and if the input is not within range,
 * an error will be thrown.
 *
 * @param {number} yearOfBirth - a year as four digits (YYYY)
 * @tag Individuals.year_of_birth.validation
 *
 * @example
 *
 * ```js
 * // in the schema
 * function validateYearOfBirth (yearOfBirth) {
 *     // ....
 * }
 * validateYearOfBirth(name)
 * ```
 *
 */

export default function validateYearOfBirth(yearOfBirth) {
  const currentYear = new Date().getUTCFullYear();
  if (yearOfBirth > currentYear) {
    return "Year of birth cannot be greater than the current year";
  } else if (yearOfBirth < 1900) {
    return "Year of birth cannot be earlier than 1900";
  } else {
    return true;
  }
}
