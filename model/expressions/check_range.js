export default function validateRange (value, min, max) {
    if (value < min) {
        return "Value cannot be smaller than" + min;
    } else if (value > max) {
        return "Value cannot be bigger than" + max;
    } else {
        return true;
    }
  }