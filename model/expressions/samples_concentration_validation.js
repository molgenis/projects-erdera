/**
 * @name Samples Concentration validation
 * @description Validation of user input in the concentration column
 * 
 * @param {number} concentration - a concentration as a decimal
 * @tag Samples.concentration.validation
*/

export default function validateConcentration (concentration) {
    if (concentration < 90) {
        return "Concentration cannot be smaller than 90 ng/µl";
    } else if (concentration > 150) {
        return "Concentration cannot be bigger than 150 ng/µl";
    } else {
        return true;
    }
  }