/**
 * @name Samples: Concentration validation
 * @description
 *
 * This expression validates the input of the concentration column.
 * Concentration measures are defined by the study protocols and
 * must be between 90 ng/µl and 150 ng/µl. If the value is outside
 * of this range, than an error message will be thrown on import
 * or entry into the webform.
 *
 * @arguments
 *
 * @param {decimal} concentration - concentration measurement (ng/µl)
 *
 * @example
 *
 * ```js
 * // in your schema
 *
 * function validateConcentration(value) {
 *    // ...
 * }
 *
 * // name of the column containing the value to validate
 * validateConcentration(concentration)
 * ```
 *
 * @tag Samples.concentration.validation
 */

export default function validateConcentration(value) {
  if (value < 90) {
    return "Concentration cannot be less than 90 ng/µl";
  } else if (value > 150) {
    return "Concentration cannot be greater than 150 ng/µl";
  } else {
    return true;
  }
}
