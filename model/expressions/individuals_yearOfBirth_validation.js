/**
 * Validation of user input in the year of birth column
 * 
 * @param {number} yearOfBirth - a year as four digits (YYYY)
 * @tag Individuals.year_of_birth.validation
*/


export default function validateYearOfBirth (yearOfBirth) {
  const currentYear = new Date().getUTCFullYear();
  if (yearOfBirth > currentYear) {
    return "Year of birth cannot be greater than the current year";
  } else if (yearOfBirth < 1900) {
    return "Year of birth cannot be earlier than 1900";
  } else {
    return true;
  }
}