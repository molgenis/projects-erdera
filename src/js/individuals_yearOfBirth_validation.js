/**
 * @name Individuals: Year of birth validation
 *
 * @description
 * This validation expression determines if the year (a number) is
 * greater than the current year or earlier than fixed year. By default,
 * the earliest year allowed is 1900, and the current year is based on
 * the UTC date.
 *
 * Input value must be in four digit year format (`YYYY`). If the value
 * is not within range, an error message will be thrown when data is
 * imported in a file or when it is entered into the web form.
 *
 * @arguments
 *
 * @param {number} yearOfBirth - a year as four digits (YYYY)
 *
 * @example
 *
 * ```js
 * validateYearOfBirth(1200) // returns error
 * validateYearOfBirth(2100) // returns error
 * validateYearOfBirth(2020) // returns true
 * ```
 *
 * @tag Individuals.year_of_birth.validation
 */

export default function validateYearOfBirth(year) {
  const currentYear = new Date().getUTCFullYear();
  if (year > currentYear) {
    return "Year of birth cannot be greater than the current year";
  } else if (year < 1900) {
    return "Year of birth cannot be earlier than 1900";
  } else {
    return true;
  }
}
