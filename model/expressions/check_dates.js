export default function validateDate (date, earliest_date) {
    try {
        const d_date = new Date(date);
        const d_earliest_date = new Date(earliest_date);
    } catch (error) {
        return "Invalid date entered";
    }
    const year = d_date.getUTCFullYear();
    const month = d_date.getUTCMonth();
    const day = d_date.getUTCDay();

    const currentYear = new Date().getUTCFullYear();
    const currentDate = new Date();
    if (d_date > currentDate) {
        return "Date cannot be in the future";
    } else if (year > currentYear) {
        return "Year cannot be greater than the current year";
    } else if (year < 1900) {
        return "Year cannot be earlier than 1900";
    } else if (d_date < d_earliest_date) {
        return "Date cannot be earlier than " + earliest_date;
    } else if (month < 0) {
        return "Month cannot be smaller than 1";
    } else if (month > 11) {
        return "Month cannot be bigger than 12";
    } else {
        return true;
    }
  }